mod proxy;

fn main() {
    proxy::start_event_loop("0.0.0.0:8080").expect("Failed to start proxy event loop");
}
