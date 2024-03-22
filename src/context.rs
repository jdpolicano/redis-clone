use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpStream};
use std::net::SocketAddr;
use crate::server::{RedisServer};

// The state of the appilication including instances of the database, a logging vec, the current client tcp strea and the socket address.
pub struct Context {
    pub server: Arc<RedisServer>,
    pub stream: TcpStream, // the connected client / peer
    pub addr: SocketAddr, // the address of this mofo
    pub logs: Vec<(Instant, String)>, // a place to log stuff - will back these up to disk eventually.
}

impl Context {
    pub fn new(server: Arc<RedisServer>, stream: TcpStream, addr: SocketAddr) -> Self {
        Context {
            server,
            stream,
            addr,
            logs: Vec::new(),
        }
    }

    pub fn log(&mut self, msg: &str) {
        self.logs.push((Instant::now(), format!("{}: {}", self.addr, msg)));
    }
}