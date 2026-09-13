/*!
    This is an elegant rewrite of proxy_ugly.rs. It breaks up the handling of connections to the proxy into 3 distinct 
    "phases": exhausting all reads, exhausing all writes, and finally re-registering the sockets with the poller, cleaning 
    up and closing sockets as necessary, or perhaps closing an entire session if necessary. This method is significantly 
    easier to reason about and avoids the need for a lot of nested if statements and repeated code. It also allows for 
    much easier implentation of features such as encryption, as these steps can simply be inserted between phases 
    without having to modify the entire loop. Additional features include least connections load balancing, graceful shutdown, 
    automatic cleanup of idle connections, automatic detection of config changes, and logging.
 */

use mio::{Poll, net, Events, Token, Interest, Waker};
use slab::Slab;
use std::io::{Read, Write};
use std::{thread, usize};
use std::net::{self as stdnet, Shutdown};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};
use ctrlc;
use std::collections::HashMap;
use tracing::{info, error, warn};

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
    last_activity: std::time::Instant,
    prev: Option<usize>,
    next: Option<usize>,
}

pub fn start_event_loop(
    bind_addr: &str, 
    healthy_servers: &Arc<RwLock<Vec<stdnet::SocketAddr>>>, 
    healthy_servers_map: &Arc<RwLock<HashMap<stdnet::SocketAddr, Arc<AtomicUsize>>>>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);
    let mut listener = net::TcpListener::bind(bind_addr.parse()?)?;
    poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
    let mut sessions = Slab::<ProxySession>::new();
    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);
    // let current_server_idx = Arc::new(AtomicUsize::new(0));
    let num_connections = Arc::new(AtomicUsize::new(0));
    let total_throughput = Arc::new(AtomicUsize::new(0));
    let mut is_shutting_down = false;

    let metrics_num_connections = num_connections.clone();
    let metrics_total_throughput = total_throughput.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(std::time::Duration::from_secs(5));
            info!("----- Load Balancer Metrics -----");
            info!(value = metrics_num_connections.load(Ordering::Relaxed), "Current number of active connections");
            info!(value = metrics_total_throughput.load(Ordering::Relaxed), "Total throughput (bytes)");
        }
    });

    let waker_clone = waker.clone();
    ctrlc::set_handler(move || {
        info!("Received Ctrl+C! Shutting down gracefully...");
        // Report WAKER_TOKEN to poll
        let _ = waker_clone.wake();
    }).expect("Error setting Ctrl-C handler");

    let mut list_head: Option<usize> = None;
    let mut list_tail: Option<usize> = None;

    loop {
        // Poll for events with a timeout of 1 second so that we can periodically check for idle connections 
        // and close them if necessary, since the poller will not return an event for connections that send no data.
        if let Err(e) = poll.poll(&mut events, Some(std::time::Duration::from_secs(1))) {
            error!(error = %e, "Poll error");
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
                                        error!("No healthy backend servers available.");
                                        continue;
                                    }
                                    let mut min_connections = usize::MAX;
                                    let mut min_connections_server: stdnet::SocketAddr = servers[0];
                                    for server in servers.iter() {
                                        // Should never panic, all servers in config are initialized in healthy_servers_map
                                        let connections = healthy_servers_map.read().unwrap().get(server).unwrap().load(Ordering::Relaxed);
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
                                        warn!(error = %e, "Failed to connect to backend server");
                                        continue;
                                    }
                                };
                                num_connections.fetch_add(1, Ordering::Relaxed);
                                healthy_servers_map.read().unwrap().get(&addr).unwrap().fetch_add(1, Ordering::Relaxed);
                                info!(client = %client_stream.peer_addr().unwrap(), current_connections = num_connections.load(Ordering::Relaxed), "Accepted new connection.");
                                let key =  sessions.insert(ProxySession {
                                    client: client_stream,
                                    server: server_stream,
                                    client_buffer: Vec::new(),
                                    server_buffer: Vec::new(),
                                    client_closed: false,
                                    server_closed: false,
                                    session_backend: addr,
                                    last_activity: std::time::Instant::now(),
                                    prev: list_tail,
                                    next: None,
                                });

                                if let Some(tail) = list_tail {
                                    sessions[tail].next = Some(key);
                                }
                                list_tail = Some(key);
                                if list_head.is_none() {
                                    list_head = Some(key);
                                }

                                let client_token = Token(key << 1);
                                let server_token = Token((key << 1) | 1);
                                poll.registry().register(&mut sessions[key].client, client_token, Interest::READABLE)?;
                                poll.registry().register(&mut sessions[key].server, server_token, Interest::READABLE)?;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                // Technically *COULD* be an error, but often is due to client aborting connection before 
                                // we can accept it. Log as a warning for now, but may need to change to debug later.
                                warn!(error = %e, "Failed to accept connection");
                                break;
                            }
                        }
                    }
                }
                WAKER_TOKEN => {
                    info!("Waker received, shutting down gracefully...");
                    is_shutting_down = true;
                }
                token => {
                    let key = token.0 >> 1;
                    let is_client = token.0 & 1 == 0;
                    let mut move_session_to_tail = false;

                    if let Some(session) = sessions.get_mut(key) {
                        // ========= Phase 1: Exhaust Reads =========
                        if is_client && event.is_readable() && !session.client_closed {
                            loop {
                                let mut buf = [0u8; 4096];
                                match session.client.read(&mut buf) {
                                    Ok(0) => {
                                        session.client_closed = true;
                                        session.last_activity = std::time::Instant::now();
                                        move_session_to_tail = true;
                                        // Only shutdown if buffer is empty otherwise we will lose data that has not 
                                        // yet been sent to the backend
                                        if session.server_buffer.is_empty() {
                                            let _ = session.server.shutdown(Shutdown::Write);
                                        }
                                        break;
                                    }
                                    Ok(n) => {
                                        session.server_buffer.extend_from_slice(&buf[..n]);
                                        total_throughput.fetch_add(n, Ordering::Relaxed);
                                        session.last_activity = std::time::Instant::now();
                                        move_session_to_tail = true;
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        warn!(error = %e, "Read error from client");
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
                                        session.last_activity = std::time::Instant::now();
                                        move_session_to_tail = true;
                                        // Only shutdown if buffer is empty otherwise we will lose data that has not 
                                        // yet been sent to the client
                                        if session.client_buffer.is_empty() {
                                            let _ = session.client.shutdown(Shutdown::Write);
                                        }
                                        break;
                                    }
                                    Ok(n) => {
                                        session.client_buffer.extend_from_slice(&buf[..n]);
                                        total_throughput.fetch_add(n, Ordering::Relaxed);
                                        session.last_activity = std::time::Instant::now();
                                        move_session_to_tail = true;
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        warn!(error = %e, "Read error from backend");
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
                                        session.last_activity = std::time::Instant::now();
                                        move_session_to_tail = true;
                                        if session.server_buffer.is_empty() {
                                            // If shutdown was not called upon client finishing read, call it now
                                            if session.client_closed {
                                                let _ = session.server.shutdown(Shutdown::Write);
                                            }
                                            break;
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        warn!(error = %e, "Write error to backend");
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
                                        session.last_activity = std::time::Instant::now();
                                        move_session_to_tail = true;
                                        if session.client_buffer.is_empty() {
                                            // If shutdown was not called upon server finishing read, call it now
                                            if session.server_closed {
                                                let _ = session.client.shutdown(Shutdown::Write);
                                            }
                                            break;
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(e) => {
                                        warn!(error = %e, "Write error to client");
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
                            if let Some(healthy_server) = healthy_servers_map.read().unwrap().get(&addr) {
                                healthy_server.fetch_sub(1, Ordering::Relaxed);
                            }
                            // healthy_servers_map.get(&addr).unwrap().fetch_sub(1, Ordering::Relaxed);
                            info!(backend = %addr, current_connections = num_connections.load(Ordering::Relaxed), "Closing session with backend.");
                            remove_from_list(&mut sessions, &mut list_head, &mut list_tail, key);
                            // Deletes session from memory as well, and Rust will drop both the client and server TcpStreams, 
                            // closing the connections, automatically dropping file descriptors, sending FIN packets to both 
                            // client and server, and deregistering the sockets from the poller. No manual deregistration or 
                            // shutdown necessary here.
                            sessions.remove(key);
                        } else {
                            safe_register(&mut poll, &mut session.client, Token(key << 1), client_needs_read, client_needs_write);
                            safe_register(&mut poll, &mut session.server, Token((key << 1) | 1), server_needs_read, server_needs_write);
                        }
                    }

                    if move_session_to_tail && sessions.contains(key) {
                        move_to_tail(&mut sessions, &mut list_head, &mut list_tail, key);
                    }
                }
            }
        }

        // Check for sessions that have been idle for more than 30 seconds and close them
        let timeout_duration = std::time::Duration::from_secs(30);
        
        while let Some(key) = list_head {
            if sessions[key].last_activity.elapsed() <= timeout_duration {
                break;
            }
            remove_from_list(&mut sessions, &mut list_head, &mut list_tail, key);
            num_connections.fetch_sub(1, Ordering::Relaxed);
            let addr = sessions[key].session_backend;
            if let Some(healthy_server) = healthy_servers_map.read().unwrap().get(&addr) {
                healthy_server.fetch_sub(1, Ordering::Relaxed);
            }
            info!(backend = %addr, current_connections = num_connections.load(Ordering::Relaxed), "Closing idle session with backend.");
            sessions.remove(key);
        }

        if is_shutting_down && sessions.is_empty() {
            info!("All sessions closed. Exiting.");
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
            warn!("Stream not found in poll registry, attempting to register instead.");
            if let Err(e) = poll.registry().register(stream, token, interest) {
                error!(error = %e, "Failed to register stream");
            }
        } else {
            error!(error = %e, "Failed to reregister stream");
        }
    }
}

fn move_to_tail(sessions: &mut Slab<ProxySession>, list_head: &mut Option<usize>, list_tail: &mut Option<usize>, key: usize) {
    if Some(key) == *list_tail {
        return; // Already at the tail
    }

    let prev = sessions[key].prev;
    let next = sessions[key].next;
    // If prev_ is prev and not None (if there is a prev)
    if let Some(prev_) = prev {
        sessions[prev_].next = next;
    } else if Some(key) == *list_head {
        // If prev_ is None and key is the head, update the head to the next node.
        *list_head = next;
    }
    // First if statement already handles the case where key is the tail, so no else if here.
    if let Some(next_) = next {
        sessions[next_].prev = prev;
    }

    sessions[key].prev = *list_tail;
    sessions[key].next = None;

    if let Some(old_tail) = *list_tail {
        sessions[old_tail].next = Some(key);
    } else {
        // If there was no old tail, that means the list was empty, so we should also set the head to this key.
        *list_head = Some(key);
    }

    *list_tail = Some(key);
}

fn remove_from_list(sessions: &mut Slab<ProxySession>, list_head: &mut Option<usize>, list_tail: &mut Option<usize>, key: usize) {
    let prev = sessions[key].prev;
    let next = sessions[key].next;

    if let Some(prev_) = prev {
        sessions[prev_].next = next;
    } else if Some(key) == *list_head {
        *list_head = next;
    }

    if let Some(next_) = next {
        sessions[next_].prev = prev;
    } else if Some(key) == *list_tail {
        *list_tail = prev;
    }
}