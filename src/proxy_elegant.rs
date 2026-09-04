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
use std::thread;
use std::net::{self as stdnet, Shutdown};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};
use ctrlc;

const LISTENER_TOKEN: Token = Token(usize::MAX);
const WAKER_TOKEN: Token = Token(usize::MAX - 1);

struct ProxySession {
    client_socket: net::TcpStream,
    backend_socket: net::TcpStream,
    client_buffer: Vec<u8>,
    backend_buffer: Vec<u8>,
    client_closed: bool,
    backend_closed: bool,
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
    let current_server_idx = Arc::new(AtomicUsize::new(0));
    let num_connections = Arc::new(AtomicUsize::new(0));
    let total_throughput = Arc::new(AtomicUsize::new(0));
    let mut is_shutting_down = false;

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
                                    let idx = current_server_idx.fetch_add(1, Ordering::Relaxed) % servers.len();
                                    servers[idx]
                                };
                                let server_stream = match net::TcpStream::connect(addr) {
                                    Ok(stream) => stream,
                                    Err(e) => {
                                        eprintln!("Failed to connect to backend server: {}", e);
                                        continue;
                                    }
                                };
                                num_connections.fetch_add(1, Ordering::Relaxed);
                                let key =  sessions.insert(ProxySession {
                                    client_socket: client_stream,
                                    backend_socket: server_stream,
                                    client_buffer: Vec::new(),
                                    backend_buffer: Vec::new(),
                                    client_closed: false,
                                    backend_closed: false,
                                });
                                let client_token = Token(key << 1);
                                let server_token = Token((key << 1) | 1);
                                poll.registry().register(&mut sessions[key].client_socket, client_token, Interest::READABLE)?;
                                poll.registry().register(&mut sessions[key].backend_socket, server_token, Interest::READABLE)?;
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

                        // ========= Phase 2: Exhaust Writes =========

                        // ========= Phase 3: Re-register Sockets and Clean Up =========
                        
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