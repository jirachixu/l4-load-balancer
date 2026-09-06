/*!
    This is an elegant rewrite of proxy_ugly.rs. It breaks up the handling of connections to the proxy into 3 distinct 
    "phases": exhausting all reads, exhausing all writes, and finally re-registering the sockets with the poller, cleaning 
    up and closing sockets as necessary, or perhaps closing an entire session if necessary. This method is significantly 
    easier to reason about and avoids the need for a lot of nested if statements and repeated code. It also allows for 
    much easier implentation of features such as encryption, as these steps can simply be inserted between phases 
    without having to modify the entire loop.
 */

use mio::{Poll, net, Events, Token, Interest, Waker};
use slab::Slab;
use std::io::{Read, Write};
use std::{thread, usize};
use std::net::{self as stdnet, Shutdown};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};
use ctrlc;
use std::collections::HashMap;

const LISTENER_TOKEN: Token = Token(usize::MAX);
const WAKER_TOKEN: Token = Token(usize::MAX - 1);

struct ProxySession {
    client: net::TcpStream,
    server: net::TcpStream,
    client_buffer: Vec<u8>,
    server_buffer: Vec<u8>,
    client_closed: bool,
    server_closed: bool,
    session_backend: stdnet::SocketAddr,
}

pub fn start_event_loop(bind_addr: &str, backends: &[stdnet::SocketAddr]) -> Result<(), Box<dyn std::error::Error>> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);
    let mut listener = net::TcpListener::bind(bind_addr.parse()?)?;
    poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
    let mut sessions = Slab::<ProxySession>::new();
    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);
    let all_servers = backends.to_vec().clone();
    let healthy_servers = Arc::new(RwLock::new(backends.to_vec().clone()));
    // let current_server_idx = Arc::new(AtomicUsize::new(0));
    let num_connections = Arc::new(AtomicUsize::new(0));
    let total_throughput = Arc::new(AtomicUsize::new(0));
    let mut is_shutting_down = false;
    let healthy_servers_map: HashMap<stdnet::SocketAddr, Arc<AtomicUsize>> = backends.to_vec().clone().into_iter()
        .map(|addr| (addr, Arc::new(AtomicUsize::new(0)))).collect();

    let health_check_healthy_servers = healthy_servers.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(std::time::Duration::from_secs(5));
            let checked_servers = all_servers.iter().filter(|server| {
                match stdnet::TcpStream::connect_timeout(server, std::time::Duration::from_secs(1)) {
                    Ok(stream) => {
                        let _ = stream.shutdown(Shutdown::Both);
                        true
                    }
                    Err(_) => false,
                }
            }).cloned().collect::<Vec<_>>();
            *health_check_healthy_servers.write().unwrap() = checked_servers;
        }
    });

    let metrics_num_connections = num_connections.clone();
    let metrics_total_throughput = total_throughput.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(std::time::Duration::from_secs(5));
            println!("----- Load Balancer Metrics -----");
            println!("Current number of active connections: {}", metrics_num_connections.load(Ordering::Relaxed));
            println!("Total throughput (bytes): {}", metrics_total_throughput.load(Ordering::Relaxed));
        }
    });

    let waker_clone = waker.clone();
    ctrlc::set_handler(move || {
        println!("Received Ctrl+C! Shutting down gracefully...");
        // Report WAKER_TOKEN to poll
        let _ = waker_clone.wake();
    }).expect("Error setting Ctrl-C handler");

    loop {
        if let Err(e) = poll.poll(&mut events, None) {
            eprintln!("Poll error: {}", e);
            continue;
        }

        for event in events.iter() {
            match event.token() {
                LISTENER_TOKEN => {
                    if is_shutting_down {
                        continue;
                    }
                    loop {
                        match listener.accept() {
                            Ok((client_stream, _)) => {
                                let addr = {
                                    let servers = healthy_servers.read().unwrap();
                                    if servers.is_empty() {
                                        eprintln!("No healthy backend servers available.");
                                        continue;
                                    }
                                    let mut min_connections = usize::MAX;
                                    let mut min_connections_server: stdnet::SocketAddr = servers[0];
                                    for server in servers.iter() {
                                        // Should never panic, all servers in config are initialized in healthy_servers_map
                                        let connections = healthy_servers_map.get(server).unwrap().load(Ordering::Relaxed);
                                        if connections < min_connections {
                                            min_connections = connections;
                                            min_connections_server = *server;
                                        }
                                    }
                                    min_connections_server
                                };
                                let server_stream = match net::TcpStream::connect(addr) {
                                    Ok(stream) => stream,
                                    Err(e) => {
                                        eprintln!("Failed to connect to backend server: {}", e);
                                        continue;
                                    }
                                };
                                num_connections.fetch_add(1, Ordering::Relaxed);
                                healthy_servers_map.get(&addr).unwrap().fetch_add(1, Ordering::Relaxed);
                                println!("Accepted new connection from {}. Total connections: {}", client_stream.peer_addr().unwrap(), num_connections.load(Ordering::Relaxed));
                                let key =  sessions.insert(ProxySession {
                                    client: client_stream,
                                    server: server_stream,
                                    client_buffer: Vec::new(),
                                    server_buffer: Vec::new(),
                                    client_closed: false,
                                    server_closed: false,
                                    session_backend: addr,
                                });
                                let client_token = Token(key << 1);
                                let server_token = Token((key << 1) | 1);
                                poll.registry().register(&mut sessions[key].client, client_token, Interest::READABLE)?;
                                poll.registry().register(&mut sessions[key].server, server_token, Interest::READABLE)?;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                eprintln!("Failed to accept connection: {}", e);
                                break;
                            }
                        }
                    }
                }
                WAKER_TOKEN => {
                    println!("Waker received, shutting down gracefully...");
                    is_shutting_down = true;
                }
                token => {
                    let key = token.0 >> 1;
                    let is_client = token.0 & 1 == 0;

                    if let Some(session) = sessions.get_mut(key) {
                        // ========= Phase 1: Exhaust Reads =========
                        if is_client && event.is_readable() && !session.client_closed {
                            loop {
                                let mut buf = [0u8; 4096];
                                match session.client.read(&mut buf) {
                                    Ok(0) => {
                                        session.client_closed = true;
                                        break;
                                    }
                                    Ok(n) => {
                                        session.server_buffer.extend_from_slice(&buf[..n]);
                                        total_throughput.fetch_add(n, Ordering::Relaxed);
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        eprintln!("Read error from client: {}", e);
                                        session.client_closed = true;
                                        let _ = session.server.shutdown(Shutdown::Write);
                                        break;
                                    }
                                }
                            }
                        }
                        if !is_client && event.is_readable() && !session.server_closed {
                            loop {
                                let mut buf = [0u8; 4096];
                                match session.server.read(&mut buf) {
                                    Ok(0) => {
                                        session.server_closed = true;
                                        break;
                                    }
                                    Ok(n) => {
                                        session.client_buffer.extend_from_slice(&buf[..n]);
                                        total_throughput.fetch_add(n, Ordering::Relaxed);
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        eprintln!("Read error from backend: {}", e);
                                        session.server_closed = true;
                                        let _ = session.client.shutdown(Shutdown::Write);
                                        break;
                                    }
                                }
                            }
                        }

                        // ========= Phase 2: Exhaust Writes =========
                        if !session.server_buffer.is_empty() {
                            loop {
                                match session.server.write(&session.server_buffer) {
                                    Ok(n) => {
                                        session.server_buffer.drain(..n);
                                        if session.server_buffer.is_empty() {
                                            break;
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        eprintln!("Write error to backend: {}", e);
                                        session.server_closed = true;
                                        session.server_buffer.clear();
                                        break;
                                    }
                                }
                            }
                        }
                        if !session.client_buffer.is_empty() {
                            loop {
                                match session.client.write(&session.client_buffer) {
                                    Ok(n) => {
                                        session.client_buffer.drain(..n);
                                        if session.client_buffer.is_empty() {
                                            break;
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        eprintln!("Write error to client: {}", e);
                                        session.client_closed = true;
                                        session.client_buffer.clear();
                                        break;
                                    }
                                }
                            }
                        }

                        // ========= Phase 3: Re-register Sockets and Clean Up =========
                        let client_needs_read = !session.client_closed;
                        let client_needs_write = !session.client_buffer.is_empty();
                        let server_needs_read = !session.server_closed;
                        let server_needs_write = !session.server_buffer.is_empty();

                        if !client_needs_read && !client_needs_write && !server_needs_read && !server_needs_write {
                            num_connections.fetch_sub(1, Ordering::SeqCst);
                            let addr = session.session_backend;
                            healthy_servers_map.get(&addr).unwrap().fetch_sub(1, Ordering::Relaxed);
                            println!("Closing session with backend {}. Total connections: {}", addr, num_connections.load(Ordering::Relaxed));
                            sessions.remove(key);
                        } else {
                            safe_register(&mut poll, &mut session.client, Token(key << 1), client_needs_read, client_needs_write);
                            safe_register(&mut poll, &mut session.server, Token((key << 1) | 1), server_needs_read, server_needs_write);
                        }
                    }
                }
            }
        }
        if is_shutting_down && sessions.is_empty() {
            println!("All sessions closed. Exiting.");
            break;
        }
    }

    Ok(())
}

/// A helper function to safely reregister a stream with the poller. If the stream is somehow not found, register it instead.
/// Also handles deregistration if both read and write interests are false. Determines the correct interest based on
/// whether the stream still needs to read or write.
pub fn safe_register(poll: &mut Poll, stream: &mut net::TcpStream, token: Token, needs_read: bool, needs_write: bool) {
    if !needs_read && !needs_write {
        let _ = poll.registry().deregister(stream);
        return;
    }

    let interest = if needs_read && needs_write {
        Interest::READABLE.add(Interest::WRITABLE)
    } else if needs_read {
        Interest::READABLE
    } else {
        Interest::WRITABLE
    };

    if let Err(e) = poll.registry().reregister(stream, token, interest) {
        if e.kind() == std::io::ErrorKind::NotFound {
            println!("Stream not found in poll registry, attempting to register instead.");
            if let Err(e) = poll.registry().register(stream, token, interest) {
                eprintln!("Failed to register stream: {}", e);
            }
        } else {
            eprintln!("Failed to reregister stream: {}", e);
        }
    }
}