use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpStream};
use std::net::SocketAddr;
use crate::database::Database;

// The state of the appilication including instances of the database, a logging vec, the current client tcp strea and the socket address.
pub struct Context {
    pub db: Arc<Database>,
    pub stream: TcpStream,
    pub addr: SocketAddr,
    pub logs: Vec<(Instant, String)>,
}

impl Context {
    pub fn new(db: Arc<Database>, stream: TcpStream, addr: SocketAddr) -> Self {
        Context {
            db,
            stream,
            addr,
            logs: Vec::new(),
        }
    }

    pub fn log(&mut self, msg: &str) {
        self.logs.push((Instant::now(), format!("{}: {}", self.addr, msg)));
    }
}