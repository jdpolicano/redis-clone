use tokio::net::{ TcpListener, TcpStream };
use std::io;
use std::time;
use std::thread;
use std::sync::Arc;
use crate::context::Context;
use crate::history::History;
use crate::connection::Connection;
use crate::database::Database;
use crate::server::ServerInfo;

#[derive(Debug)]
pub struct Listener {
    listener: TcpListener, // the socket we've bound to
    db: Arc<Database>, // the database we're running
    history: Arc<History>, // the server's connected replicas and transaction history
    info: Arc<ServerInfo>, // info about the server that is currently handling requests.
}


impl Listener {
    pub fn new(listener: TcpListener, db: Database, history: History, info: ServerInfo) -> Self {
        let db = Arc::new(db);
        let history = Arc::new(history);
        let info = Arc::new(info);

        Self {
            listener,
            db,
            history,
            info
        }
    }

    pub async fn accept(&self) -> io::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => return Ok(stream),
                Err(e) if backoff > 64 => return Err(e),
                _ => {
                    thread::sleep(time::Duration::from_secs(backoff));
                    backoff *= 2;
                }
            }
        }
    }

    pub async fn run(&self) -> io::Result<()> {
        
        loop {
            let stream = self.accept().await?; 
            let connection = Connection::new(stream);
            
            let mut ctx = Context::new(
                connection, 
                self.db.clone(), 
                self.history.clone(), 
                self.info.clone()
            );

            tokio::spawn(async move {
                if let Err(e) = ctx.handle().await {
                    println!("{:?}", e);
                }
            });
        }
    }
}