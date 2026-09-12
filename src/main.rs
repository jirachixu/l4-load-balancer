mod proxy_elegant;
use rlimit;
use serde::Deserialize;
use toml;
use std::{fs, path::Path, time::Duration, thread, collections::HashMap};
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, notify::RecursiveMode};
use std::sync::{Arc, RwLock, atomic::AtomicUsize};

#[derive(Deserialize, Debug)]
struct Config {
    bind_address: String,
    backends: Vec<Backend>,
}

#[derive(Deserialize, Debug)]
struct Backend {
    address: String,
}

fn main() {
    // Since Linux has a default limit of 1024 open files, we need to increase it to handle more concurrent connections.
    const MAX_OPEN_FILES: u64 = 65535;
    if let Err(e) = rlimit::setrlimit(rlimit::Resource::NOFILE, MAX_OPEN_FILES, MAX_OPEN_FILES) {
        eprintln!("Failed to set resource limit: {}", e);
        eprintln!("You may need to run the program with elevated privileges or adjust your system's limits.");
    }

    let contents = fs::read_to_string("config.toml").expect("Failed to read config!");
    let config: Config = toml::from_str(&contents).expect("Failed to parse config!");

    let backends: Vec<std::net::SocketAddr> = config.backends.iter().filter_map(|backend| {
        match backend.address.parse::<std::net::SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(e) => {
                eprintln!("Invalid backend address '{}': {}", backend.address, e);
                None
            }
        }
    }).collect();

    let all_servers = Arc::new(RwLock::new(backends.clone()));
    let healthy_servers = Arc::new(RwLock::new(backends.clone()));
    let healthy_servers_map: Arc<RwLock<HashMap<std::net::SocketAddr, Arc<AtomicUsize>>>> = Arc::new(RwLock::new(
        backends.clone().iter().map(|&addr| (addr, Arc::new(AtomicUsize::new(0)))).collect()
    ));
    let health_check_all_servers = all_servers.clone();

    let health_check_healthy_servers = healthy_servers.clone();
    let health_check_healthy_servers_map = healthy_servers_map.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(std::time::Duration::from_secs(5));
            let checked_servers = health_check_all_servers.read().unwrap().iter().filter(|server| {
                match std::net::TcpStream::connect_timeout(server, std::time::Duration::from_secs(1)) {
                    Ok(stream) => {
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        true
                    }
                    Err(_) => false,
                }
            }).cloned().collect::<Vec<_>>();
            *health_check_healthy_servers.write().unwrap() = checked_servers;
            // Remove backends that are no longer present in the new configuration to prevent 
            // memory leaks and ensure that the healthy_servers_map only contains current backends.
            // Do this in the health check thread to avoid race conditions with the config reload thread.
            health_check_healthy_servers_map.write().unwrap().retain(|server, _| health_check_all_servers.read().unwrap().contains(server));
        }
    });

    let healthy_servers_map_clone = healthy_servers_map.clone();
    let all_servers_clone = all_servers.clone();

    // This thread watches for changes in the working directory and reloads the configuration if config.toml is modified.
    thread::spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut debouncer = new_debouncer(Duration::from_millis(200), tx).unwrap();
        // NonRecursive watches the specified path but does not watch its subdirectories.
        debouncer.watcher().watch(Path::new("."), RecursiveMode::NonRecursive).unwrap();

        println!("Watching for changes in config.toml...");

        for result in rx {
            match result {
                DebounceEventResult::Ok(events) => {
                    for event in events {
                        if event.path.ends_with("config.toml") {
                            println!("Detected change in config.toml: {:?}", event);

                            let contents = fs::read_to_string("config.toml").expect("Failed to read config!");
                            let new_config: Config = toml::from_str(&contents).expect("Failed to parse config!");

                            let new_backends: Vec<std::net::SocketAddr> = new_config.backends.iter().filter_map(|backend| {
                                match backend.address.parse::<std::net::SocketAddr>() {
                                    Ok(addr) => Some(addr),
                                    Err(e) => {
                                        eprintln!("Invalid backend address '{}': {}", backend.address, e);
                                        None
                                    }
                                }
                            }).collect();

                            // Update this first to prevent race conditions where the health check thread updates healthy_servers
                            // using all_servers and a user connects the millisecond before the map is updated
                            for backend in new_backends.iter() {
                                healthy_servers_map_clone.write().unwrap().entry(*backend).or_insert_with(|| Arc::new(AtomicUsize::new(0)));
                            }

                            *all_servers_clone.write().unwrap() = new_backends.clone();
                        }
                    }
                }
                DebounceEventResult::Err(e) => {
                    eprintln!("Error watching config.toml: {:?}", e);
                }
            }
        }
    });

    println!("Starting proxy server on {} with backends: {:?}", config.bind_address, backends);

    proxy_elegant::start_event_loop(&config.bind_address, &healthy_servers, &healthy_servers_map).expect("Failed to start proxy event loop");
}
