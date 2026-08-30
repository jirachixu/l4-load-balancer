mod proxy;
use rlimit;
use clap::Parser;

#[derive(Parser)]
struct Config {
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind_addr: String,

    // Accept multiple IPs separated by spaces: --backends 127.0.0.1:9000 127.0.0.1:9001
    #[arg(num_args = 1.., long, value_delimiter = ' ')]
    backends: Vec<String>,
}

fn main() {
    // Since Linux has a default limit of 1024 open files, we need to increase it to handle more concurrent connections.
    const MAX_OPEN_FILES: u64 = 65535;
    if let Err(e) = rlimit::setrlimit(rlimit::Resource::NOFILE, MAX_OPEN_FILES, MAX_OPEN_FILES) {
        eprintln!("Failed to set resource limit: {}", e);
        eprintln!("You may need to run the program with elevated privileges or adjust your system's limits.");
    }

    let config = Config::parse();
    println!("Starting proxy server on {} with backends: {:?}", config.bind_addr, config.backends);

    let backends: Vec<std::net::SocketAddr> = config.backends.iter().filter_map(|backend| {
        match backend.parse::<std::net::SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(e) => {
                eprintln!("Invalid backend address '{}': {}", backend, e);
                None
            }
        }
    }).collect();

    proxy::start_event_loop(&config.bind_addr, &backends).expect("Failed to start proxy event loop");
}
