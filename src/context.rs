use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpStream};
use std::net::SocketAddr;
use crate::server::{RedisServer};

// The state of the appilication including instances of the database, a logging vec, the current client tcp strea and the socket address.
pub struct Context {
    pub server: Arc<RedisServer>,
    pub stream: TcpStream, // the connected client / peer. Inside an option so that ownership can be taken out of the context.
    pub addr: SocketAddr, // the address of this mofo
    pub keep_connection_alive: bool, // whether to keep this connection as a replica.
    pub logs: Vec<(Instant, String)>, // a place to log stuff - will back these up to disk eventually.
}

impl Context {
    pub fn new(server: Arc<RedisServer>, stream: TcpStream, addr: SocketAddr) -> Self {
        Context {
            server,
            stream,
            addr,
            keep_connection_alive: false,
            logs: Vec::new(),
        }
    }

    pub async fn preserve_stream(self) {
        self.server.add_replica(self.stream).await;
    }

    pub fn keep_alive(&mut self) {
        self.keep_connection_alive = true;
    }

    pub fn log(&mut self, msg: &str) {
        self.logs.push((Instant::now(), format!("{}: {}", self.addr, msg)));
    }
}