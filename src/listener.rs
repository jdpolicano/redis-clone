use tokio::net::{ TcpListener, TcpStream };
use std::io;
use std::time;
use std::thread;
use std::sync::Arc;
use crate::context::{Context, Handle};
use crate::history::History;
use crate::connection::Connection;
use crate::database::Database;
use crate::server::ServerInfo;
use crate::client::RedisClient;
use crate::protocol::ReplicationProtocol;

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

    pub async fn run(&self) -> io::Result<()> {
        // check if the server is a replica
        if self.info.is_replica() {
            // if it is, we need to connect to the master server and start listening for updates.
            // this will be implemented later.
            self.replicate_before_listen().await?;
            loop {
                let stream = self.accept().await?; 
                let connection = Connection::new(stream);
                self.listen_limited(connection);
            }
        } 

        loop {
            let stream = self.accept().await?; 
            let connection = Connection::new(stream);
            self.listen_all(connection);
        }
    }

    async fn accept(&self) -> io::Result<TcpStream> {
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

    // listen to connections with unlimited functionality.
    fn listen_all(&self, stream: Connection) {
        let ctx = Context::new(
            stream, 
            self.db.clone(), 
            self.history.clone(), 
            self.info.clone()
        );
        
        tokio::spawn(async move {
            ctx.handle_all().await
        });
    }

    fn listen_limited(&self, stream: Connection) {
        let ctx = Context::new(
            stream, 
            self.db.clone(), 
            self.history.clone(), 
            self.info.clone()
        );
        
        tokio::spawn(async move {
            ctx.handle_limited().await
        });
    }

    async fn replicate_before_listen(&self) -> io::Result<()> {
        println!("begin negotiation...");
        let tcp_socket = TcpStream::connect(self.info.get_master_host().unwrap()).await?;
        let mut stream = Connection::new(tcp_socket);
        let client = RedisClient::from_stream(&mut stream);
        let listening_port = self.listener
            .local_addr()?
            .port()
            .to_string();
        
        let handle = Handle {
            database: self.db.clone(),
            history: self.history.clone(),
            info: self.info.clone()
        };

        let mut protocol = ReplicationProtocol::new(
            client,
            listening_port,
            handle
        );

        protocol.start().await?;

        println!("begin listening...");
        self.listen_all(stream);

        Ok(())
    }
}