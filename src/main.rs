mod proxy_elegant;
use rlimit;
use serde::Deserialize;
use toml;
use std::fs;

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

    println!("Starting proxy server on {} with backends: {:?}", config.bind_address, backends);

    proxy_elegant::start_event_loop(&config.bind_address, &backends).expect("Failed to start proxy event loop");
}
