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
    
}