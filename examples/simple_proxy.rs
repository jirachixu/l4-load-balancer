// This example demonstrates a simple TCP proxy that forwards data between a client and a server.
use std::net::{TcpListener, TcpStream, Shutdown, SocketAddr};
use std::{io, thread, io::Write};
use std::os::fd::AsRawFd;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use ctrlc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    // Set up a Ctrl-C handler to gracefully shut down the server when the user interrupts the program.
    // The ctrlc crate provides a way to handle Ctrl-C signals in a cross-platform manner.
    ctrlc::set_handler(move || {
        println!("Ctrl-C received, shutting down...");
        running_clone.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    if let Err(e) = run(&running) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}

fn run(running: &Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    // Set the listener to non-blocking mode so that it can accept connections without blocking the main thread.
    // Otherwise, the accept() call would block indefinitely if there are no incoming connections,
    // preventing the program from checking the running flag and shutting down gracefully.
    listener.set_nonblocking(true)?;
    // Arc and AtomicUsize are used to keep track of the current server index in a thread-safe manner. 
    // Standard operations like x += 1 actually take 3 steps at CPU level: read, increment, write. 
    // If multiple threads are doing this at the same time, they can interfere with each other and cause incorrect results.
    // AtomicUsize provides atomic operations that ensure that these steps are done as a single, indivisible operation.
    // This allows multiple threads to safely increment the index and select the next server in a round-robin fashion.
    // Arc wraps data in a pointer tracking the number of references to it, 
    // allowing multiple threads to share ownership of the same data.
    let current_index = Arc::new(AtomicUsize::new(0));
    let live_traffic = Arc::new(AtomicUsize::new(0));
    // RwLock is used to allow an arbitrary number of readers until a writer is present. 
    // If a writer is present, it will block all readers until it is done.
    let servers = Arc::new(
        vec!["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004", "127.0.0.1:9005"]
    );
    // The healthy_servers variable is used to keep track of the servers that are currently reachable.
    let healthy_servers = Arc::new(RwLock::new(
        servers.iter().copied().collect::<Vec<&str>>()
    ));
    let healthy_servers_clone = Arc::clone(&healthy_servers);
    let health_running = Arc::clone(running);

    thread::spawn(move || {
        // This thread is responsible for monitoring the health of the servers. 
        // It checks if each server is reachable and updates the list of servers accordingly.
        while health_running.load(Ordering::SeqCst) {
            let checked_servers = servers.iter().copied().filter(|server| {
                let Ok(addr) = server.parse::<SocketAddr>() else {
                    eprintln!("Invalid server address: {}", server);
                    return false;
                };
                TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(4)).is_ok()
            }).collect::<Vec<&str>>();
            // Update the list of healthy servers.
            *healthy_servers_clone.write().unwrap() = checked_servers;
            // Sleeps to avoid spamming servers with health checks.
            thread::sleep(std::time::Duration::from_secs(5));
        }
    });

    // The main loop of the proxy server. It continuously accepts new client connections and 
    // spawns a new thread to handle each connection.
    while running.load(Ordering::SeqCst) {
        // Attempt to accept a new client connection.
        let client_stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No incoming connection, sleep for a short duration to avoid high CPU usage.
                thread::sleep(std::time::Duration::from_millis(100));
                continue;
            },
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
                continue;
            },
        };
        println!("Accepted connection from file descriptor {}, address: {}", 
            client_stream.as_raw_fd(), client_stream.peer_addr()?);
        // Clone the Arc to share the current_index between threads. 
        // Each thread will have its own reference to the same AtomicUsize or Arc instance, 
        // allowing them to safely increment the index and select the next server in a round-robin fashion.
        let current_index = Arc::clone(&current_index);
        let healthy_servers = Arc::clone(&healthy_servers);
        let running = Arc::clone(&running);
        let live_traffic = Arc::clone(&live_traffic);
        // keep track of the number of live connections. This is useful for graceful shutdown,
        // as it allows the server to wait for all active connections to finish before exiting.
        live_traffic.fetch_add(1, Ordering::SeqCst);
        // Spawn a new thread to handle the connection so that the main thread can continue accepting new connections.
        thread::spawn(move || {
            // Must be unwrapped to assert a healthy read lock and expose the inner vector.
            // Clone the vector to avoid holding the lock while handling the connection, 
            // which could block other threads (the health check write) from accessing the healthy_servers.
            // Clone ensures that the type is Vec<&str> instead of RwLockReadGuard<Vec<&str>>.
            // I.e., the lock is released.
            let healthy_servers = healthy_servers.read().unwrap().clone();
            if let Err(e) = handle_connection(client_stream, &healthy_servers, &current_index, &running) {
                eprintln!("Error handling connection: {}", e);
            }
            live_traffic.fetch_sub(1, Ordering::SeqCst);
        });
    }

    println!("Shutting down server. Live traffic: {}", live_traffic.load(Ordering::SeqCst));

    // Wait for all live connections to finish before exiting (graceful shutdown).
    while live_traffic.load(Ordering::SeqCst) > 0 {
        println!("Waiting for {} live connections to finish...", live_traffic.load(Ordering::SeqCst));
        thread::sleep(std::time::Duration::from_millis(100));
    }

    Ok(())
}

/// Handle a single client connection by connecting to a server and forwarding data between the client and server.
/// Done in a separate thread to allow the main thread to continue accepting new connections.
fn handle_connection(
    mut client_stream: TcpStream, 
    healthy_servers: &[&str], 
    current_index: &AtomicUsize,
    running: &AtomicBool
) -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the server
    let mut server_stream = match connect_until_success(healthy_servers, current_index) {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to any server: {}", e);
            send_connection_error(client_stream)?;
            return Err(e);
        },
    };

    if !running.load(Ordering::SeqCst) {
        // If the server is shutting down, close the client connection and return early.
        let _ = server_stream.shutdown(Shutdown::Both);
        let _ = client_stream.shutdown(Shutdown::Both);
        return Ok(());
    }

    // Clone the streams to allow simultaneous reading and writing in separate threads
    // The clones are simply references to the same underlying socket, so they can be used to read and write data concurrently.
    let mut client_clone = client_stream.try_clone()?;
    let mut server_clone = server_stream.try_clone()?;

    // Spawn two threads to handle the bidirectional data transfer between the client and server.
    let client_to_server = thread::spawn(move || {
        let _ = client_stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
        // Copy data from the client to the server
        if let Err(e) = io::copy(&mut client_stream, &mut server_stream) {
            eprintln!("Error copying data from client to server: {}", e);
        }
        if let Err(e) = server_stream.shutdown(Shutdown::Write) {
            eprintln!("Error shutting down server stream: {}", e);
        }
        Ok::<(), io::Error>(())
    });

    let server_to_client = thread::spawn(move || {
        let _ = server_clone.set_read_timeout(Some(std::time::Duration::from_secs(10)));
        // Copy data from the server to the client
        if let Err(e) = io::copy(&mut server_clone, &mut client_clone) {
            eprintln!("Error copying data from server to client: {}", e);
        }
        if let Err(e) = client_clone.shutdown(Shutdown::Write) {
            eprintln!("Error shutting down client stream: {}", e);
        }
        Ok::<(), io::Error>(())
    });

    // Wait for both threads to finish
    let _ = client_to_server.join();
    let _ = server_to_client.join();

    Ok(())
}

/// Loop until a successful connection is made to the server. If the connection fails, 
/// it will try the next server in the list, until all servers have been tried.
fn connect_until_success(healthy_servers: &[&str], current_index: &AtomicUsize) -> Result<TcpStream, Box<dyn std::error::Error>> {
    if healthy_servers.is_empty() {
        return Err("No servers available".into());
    }
    for _ in 0..healthy_servers.len() {
        // Fetch the current index and increment it atomically after returning the current value, 
        // then use modulo to wrap around if it exceeds the number of servers.
        let server = healthy_servers[current_index.fetch_add(1, Ordering::SeqCst) % healthy_servers.len()];
        let addr = match server.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!("Invalid server address: {}; Error: {}. Trying next server...", server, e);
                continue;
            }
        };
        match TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(4)) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                eprintln!("Failed to connect to server {}: {}. Trying next server...", server, e);
            }
        }
    }
    Err("All servers failed to connect".into())
}

/// Send a connection error response to the client.
fn send_connection_error(mut client_stream: TcpStream) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\n\
        Content-Type: text/plain\r\n\
        Content-Length: {}\r\n\
        Connection: close\r\n\
        \r\n",
        "No backend available.\n".len()
    );
    client_stream.write_all(response.as_bytes())?;
    client_stream.write_all(b"No backend available.\n")?;
    client_stream.shutdown(Shutdown::Write)?;
    Ok(())
}