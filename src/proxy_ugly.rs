use mio::{Poll, net, Events, Token, Interest, Waker};
use slab::Slab;
use std::io::{Read, Write};
use std::thread;
use std::net::{self as stdnet, Shutdown};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};
use ctrlc;

// Use usize::MAX for the listener token and usize::MAX - 1 for the waker token to avoid conflicts with other tokens.
const LISTENER_TOKEN: Token = Token(usize::MAX);
const WAKER_TOKEN: Token = Token(usize::MAX - 1);

/// A structure representing a proxy session between a client and a server.
/// This helps the proxy accurately mirror the state of the connection between the client and server, 
/// including any buffered data and whether either side has closed the connection.
/// This is important for bidirectional data transfer and graceful shutdown of the connection.
struct ProxySession {
    client: net::TcpStream,
    server: net::TcpStream,
    client_buffer: Vec<u8>,
    server_buffer: Vec<u8>,
    client_closed: bool,
    server_closed: bool,
}

pub fn start_event_loop(bind_addr: &str, backends: &Vec<std::net::SocketAddr>) -> Result<(), Box<dyn std::error::Error>> {
    // Polls for readiness events (is the socket ready to read/write), watching file descriptors for events. 
    // This is the main entry point to Mio's event loop.
    let mut poll = Poll::new()?;
    // Holds ready events when the loop is running. 
    // This is a collection of events that have occurred since the last time the event loop was run.
    let mut events = Events::with_capacity(1024);
    // parse() parses bind_addr into a SocketAddr.
    let mut listener = net::TcpListener::bind(bind_addr.parse()?)?;
    // Gets the registry from the Poll instance and registers the listener with a token and interest in readable events.
    poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
    // Waker is used to wake up the event loop from another thread.
    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);
    let waker_clone = waker.clone();

    // This HashMap is necessary because mio only provides a Token to identify which socket is ready, 
    // but it doesn't provide the actual socket itself.
    let mut sessions: Slab<ProxySession> = Slab::new();

    let healthy_servers = Arc::new(RwLock::new(backends.clone()));
    let all_servers = backends.clone();
    let current_server_index = Arc::new(AtomicUsize::new(0));

    let health_check_healthy_servers = healthy_servers.clone();

    // Health check thread, same as in simple_proxy
    thread::spawn(move || {
        loop {
            let checked_servers = all_servers.iter().filter(|addr| {
                match stdnet::TcpStream::connect_timeout(addr, std::time::Duration::from_secs(3)) {
                    Ok(stream) => {
                        // If we can connect, the server is healthy. Close the connection immediately.
                        let _ = stream.shutdown(Shutdown::Both);
                        true
                    }
                    Err(_) => false,
                }
            }).cloned().collect::<Vec<std::net::SocketAddr>>();
            *health_check_healthy_servers.write().unwrap() = checked_servers;
            thread::sleep(std::time::Duration::from_secs(5));
        }
    });

    let num_connections = Arc::new(AtomicUsize::new(0));
    let total_throughput = Arc::new(AtomicUsize::new(0));

    let metrics_num_connections = num_connections.clone();
    let metrics_total_throughput = total_throughput.clone();

    // Metrics thread to print the number of active connections and total throughput every 5 seconds, avoiding console spam.
    thread::spawn(move || {
        loop {
            // Use Ordering::Relaxed here because we don't need a strict ordering of operations for metrics; 
            // we just want to read the current values. Orering::SeqCst is used in the main loop to ensure that 
            // increments and decrements are seen in the correct order that they actually occurred, which takes more 
            // time and CPU.
            let active_connections = metrics_num_connections.load(Ordering::Relaxed);
            let throughput = metrics_total_throughput.load(Ordering::Relaxed);
            println!("----- Load Balancer Metrics -----");
            println!("Current number of active connections: {}", active_connections);
            println!("Total throughput (bytes): {}", throughput);
            thread::sleep(std::time::Duration::from_secs(5));
        }
    });

    ctrlc::set_handler(move || {
        println!("Ctrl-C received, shutting down...");
        let _ = waker_clone.wake();
    }).expect("Error setting Ctrl-C handler");

    let mut is_shutting_down = false;

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            match event.token() {
                WAKER_TOKEN => {
                    is_shutting_down = true;
                }
                LISTENER_TOKEN => {
                    if !is_shutting_down {
                        loop {
                            match listener.accept() {
                                Ok((client_stream, _)) => {
                                    let addr = {
                                        let servers = healthy_servers.read().unwrap();
                                        if servers.is_empty() {
                                            eprintln!("No healthy servers available");
                                            continue;
                                        }
                                        let index = current_server_index.fetch_add(1, Ordering::SeqCst) % servers.len();
                                        servers[index]
                                    };
                                    let server_stream = net::TcpStream::connect(addr)?;
                                    num_connections.fetch_add(1, Ordering::SeqCst);
                                    let key = sessions.insert(ProxySession {
                                        client: client_stream,
                                        server: server_stream,
                                        client_buffer: Vec::new(),
                                        server_buffer: Vec::new(),
                                        client_closed: false,
                                        server_closed: false,
                                    });
                                    // Slab is a pre-allocated array that allows for efficient insertion and removal of elements.
                                    // We calculate the tokens for the client and server streams based on the key in the slab.
                                    // The client token is the key shifted left by 1, and the server token is the key shifted left 
                                    // by 1 and then OR'd with 1 (flips the least significant bit, adding 1 in this case since a 
                                    // left bit shift turns the least significant bit to 0, i.e. an even number). 
                                    // We can find the key using the token by doing a right shift.
                                    let client_token = Token(key << 1);
                                    let server_token = Token((key << 1) | 1);
                                    poll.registry().register(&mut sessions[key].client, client_token, Interest::READABLE)?;
                                    poll.registry().register(&mut sessions[key].server, server_token, Interest::READABLE)?;
                                }
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("Failed to accept connection: {}", e);
                                }
                            }
                        }
                    }
                }
                _token => {
                    let key = _token.0 >> 1;
                    let client_token = Token(key << 1);
                    let server_token = Token((key << 1) | 1);
                    let session = &mut sessions[key];
                    // Look at the lowest bit of the token
                    match _token.0 & 1 {
                        0 => {
                            // This is the client token
                            // Check whether it's a read or write event.
                            if event.is_readable() {
                                loop {
                                    // Read data from the client into a buffer. If the client has closed the connection, 
                                    // mark it as closed and deregister it from the poller. If data is read, 
                                    // append it to the server buffer and reregister the server for writable events. 
                                    // If the read would block, break out of the loop. If there's an error, log it and break.
                                    let mut buf = [0; 4096];
                                    match session.client.read(&mut buf) {
                                        Ok(0) => {
                                            // Client closed the connection
                                            session.client_closed = true;
                                            if session.server_buffer.is_empty() {
                                                // Shut down the server's write half to signal that no more data will be sent to it.
                                                // Server can still be read from, so don't deregister it yet.
                                                match session.server.shutdown(Shutdown::Write) {
                                                    Ok(()) => {}
                                                    Err(ref e) if e.kind() == std::io::ErrorKind::NotConnected => {
                                                        // The server might have already closed the connection, so we can ignore this error.
                                                    }
                                                    Err(ref e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                                        // The server might have already closed the connection, so we can ignore this error.
                                                    }
                                                    Err(e) => eprintln!("Failed to shutdown server: {}", e),
                                                }
                                                let _ = poll.registry().deregister(&mut session.client);
                                            } else {
                                                // There is still data to send to the server; make sure it's registered for WRITABLE.
                                                if let Err(e) = poll.registry().reregister(&mut session.server, server_token, Interest::READABLE | Interest::WRITABLE) {
                                                    if e.kind() == std::io::ErrorKind::NotFound {
                                                        let _ = poll.registry().register(&mut session.server, server_token, Interest::READABLE | Interest::WRITABLE);
                                                    }
                                                }
                                            }
                                            break;
                                        }
                                        Ok(n) => {
                                            total_throughput.fetch_add(n, Ordering::SeqCst);
                                            session.server_buffer.extend_from_slice(&buf[..n]);
                                            
                                            // Edge-triggered epoll fix: Try writing immediately
                                            loop {
                                                match session.server.write(&session.server_buffer) {
                                                    Ok(0) => break,
                                                    Ok(nw) => {
                                                        session.server_buffer.drain(..nw);
                                                        if session.server_buffer.is_empty() && session.client_closed {
                                                            let _ = session.server.shutdown(Shutdown::Write);
                                                            let _ = poll.registry().deregister(&mut session.server);
                                                        }
                                                        if session.server_buffer.is_empty() {
                                                            break;
                                                        }
                                                    }
                                                    Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                                                    Err(e) => {
                                                        eprintln!("Failed to write to server immediately: {}", e);
                                                        break;
                                                    }
                                                }
                                            }

                                            let mut interest = Interest::READABLE;
                                            if !session.server_buffer.is_empty() {
                                                interest |= Interest::WRITABLE;
                                            }
                                            if let Err(e) = poll.registry().reregister(&mut session.server, server_token, interest) {
                                                if e.kind() == std::io::ErrorKind::NotFound {
                                                    let _ = poll.registry().register(&mut session.server, server_token, interest);
                                                }
                                            }
                                        }
                                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to read from client: {}", e);
                                            break;
                                        }
                                    }
                                }
                            } 
                            if event.is_writable() {
                                if !session.client_buffer.is_empty() {
                                    loop {
                                        match session.client.write(&session.client_buffer) {
                                            Ok(0) => {
                                                // This shouldn't happen, but if it does, we treat it as a closed connection.
                                                session.client_closed = true;
                                                if session.server_buffer.is_empty() {
                                                    // If the server buffer is empty, we can shut down the server's write half.
                                                    match session.server.shutdown(Shutdown::Write) {
                                                        Ok(()) => {}
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::NotConnected => {
                                                            // The server might have already closed the connection, so we can ignore this error.
                                                        }
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                                            // The server might have already closed the connection, so we can ignore this error.
                                                        }
                                                        Err(e) => eprintln!("Failed to shutdown server: {}", e),
                                                    }
                                                    let _ = poll.registry().deregister(&mut session.client);
                                                }
                                                break;
                                            }
                                            Ok(n) => {
                                                session.client_buffer.drain(..n);
                                                if session.client_buffer.is_empty() && session.server_closed {
                                                    // Buffer fully drained and server already sent EOF; finalize the half-close.
                                                    match session.client.shutdown(Shutdown::Write) {
                                                        Ok(()) => {}
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::NotConnected => {}
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
                                                        Err(e) => eprintln!("Failed to shutdown client: {}", e),
                                                    }
                                                    let _ = poll.registry().deregister(&mut session.server);
                                                } else {
                                                    let mut interest = Interest::READABLE;
                                                    if !session.client_buffer.is_empty() {
                                                        interest |= Interest::WRITABLE;
                                                    }
                                                    if let Err(e) = poll.registry().reregister(&mut session.client, client_token, interest) {
                                                        if e.kind() == std::io::ErrorKind::NotFound {
                                                            let _ = poll.registry().register(&mut session.client, client_token, interest);
                                                        }
                                                    }
                                                }
                                            }
                                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => { break; }
                                            Err(e) => {
                                                eprintln!("Failed to write to client: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            // This is the server token
                            if event.is_readable() {
                                loop {
                                    let mut buf = [0; 4096];
                                    match session.server.read(&mut buf) {
                                        Ok(0) => {
                                            // Server closed the connection
                                            session.server_closed = true;
                                            if session.client_buffer.is_empty() {
                                                match session.client.shutdown(Shutdown::Write) {
                                                    Ok(()) => {}
                                                    Err(ref e) if e.kind() == std::io::ErrorKind::NotConnected => {
                                                        // The client might have already closed the connection, so we can ignore this error.
                                                    }
                                                    Err(ref e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                                        // The client might have already closed the connection, so we can ignore this error.
                                                    }
                                                    Err(e) => eprintln!("Failed to shutdown client: {}", e),
                                                }
                                                let _ = poll.registry().deregister(&mut session.server);
                                            } else {
                                                // There is still data to send to the client; make sure it's registered for WRITABLE.
                                                if let Err(e) = poll.registry().reregister(&mut session.client, client_token, Interest::READABLE | Interest::WRITABLE) {
                                                    if e.kind() == std::io::ErrorKind::NotFound {
                                                        let _ = poll.registry().register(&mut session.client, client_token, Interest::READABLE | Interest::WRITABLE);
                                                    }
                                                }
                                            }
                                            break;
                                        }
                                        Ok(n) => {
                                            total_throughput.fetch_add(n, Ordering::SeqCst);
                                            session.client_buffer.extend_from_slice(&buf[..n]);
                                            
                                            // Edge-triggered epoll fix: Try writing immediately
                                            loop {
                                                match session.client.write(&session.client_buffer) {
                                                    Ok(0) => break,
                                                    Ok(nw) => {
                                                        session.client_buffer.drain(..nw);
                                                        if session.client_buffer.is_empty() && session.server_closed {
                                                            let _ = session.client.shutdown(Shutdown::Write);
                                                            let _ = poll.registry().deregister(&mut session.client);
                                                        }
                                                        if session.client_buffer.is_empty() {
                                                            break;
                                                        }
                                                    }
                                                    Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                                                    Err(e) => {
                                                        eprintln!("Failed to write to client immediately: {}", e);
                                                        break;
                                                    }
                                                }
                                            }

                                            let mut interest = Interest::READABLE;
                                            if !session.client_buffer.is_empty() {
                                                interest |= Interest::WRITABLE;
                                            }
                                            if let Err(e) = poll.registry().reregister(&mut session.client, client_token, interest) {
                                                if e.kind() == std::io::ErrorKind::NotFound {
                                                    let _ = poll.registry().register(&mut session.client, client_token, interest);
                                                }
                                            }
                                        }
                                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to read from server: {}", e);
                                            break;
                                        }
                                    }
                                }
                            } 
                            if event.is_writable() {
                                if !session.server_buffer.is_empty() {
                                    loop {
                                        match session.server.write(&session.server_buffer) {
                                            Ok(0) => {
                                                // This shouldn't happen, but if it does, we treat it as a closed connection.
                                                session.server_closed = true;
                                                if session.client_buffer.is_empty() {
                                                    match session.client.shutdown(Shutdown::Write) {
                                                        Ok(()) => {}
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::NotConnected => {
                                                            // The client might have already closed the connection, so we can ignore this error.
                                                        }
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                                            // The client might have already closed the connection, so we can ignore this error.
                                                        }
                                                        Err(e) => eprintln!("Failed to shutdown client: {}", e),
                                                    }
                                                    let _ = poll.registry().deregister(&mut session.server);
                                                }
                                                break;
                                            }
                                            Ok(n) => {
                                                session.server_buffer.drain(..n);
                                                if session.server_buffer.is_empty() && session.client_closed {
                                                    // Buffer fully drained and client already sent EOF; finalize the half-close.
                                                    match session.server.shutdown(Shutdown::Write) {
                                                        Ok(()) => {}
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::NotConnected => {}
                                                        Err(ref e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
                                                        Err(e) => eprintln!("Failed to shutdown server: {}", e),
                                                    }
                                                    let _ = poll.registry().deregister(&mut session.client);
                                                } else {
                                                    let mut interest = Interest::READABLE;
                                                    if !session.server_buffer.is_empty() {
                                                        interest |= Interest::WRITABLE;
                                                    }
                                                    if let Err(e) = poll.registry().reregister(&mut session.server, server_token, interest) {
                                                        if e.kind() == std::io::ErrorKind::NotFound {
                                                            let _ = poll.registry().register(&mut session.server, server_token, interest);
                                                        }
                                                    }
                                                }
                                            }
                                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => { break; }
                                            Err(e) => {
                                                eprintln!("Failed to write to server: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if session.server_closed && session.client_closed && session.server_buffer.is_empty() && session.client_buffer.is_empty() {
                        num_connections.fetch_sub(1, Ordering::SeqCst);
                        // Both sides have closed the connection, remove the session from the slab.
                        sessions.remove(key);
                    }
                }
            }
        }
        if is_shutting_down && sessions.is_empty() {
            println!("All sessions closed, exiting event loop gracefully.");
            break;
        }
    }
    Ok(())
}