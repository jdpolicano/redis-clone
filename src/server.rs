// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncWriteExt, AsyncReadExt };
use tokio::sync::{Mutex};
use bytes::BytesMut;
use std::io::{ self };
use std::net::SocketAddr;
use crate::resp::{ Resp, RespParser, RespEncoder};
use crate::database::{ Database };
use crate::arguments::{ ServerArguments };

pub struct ServerInfo {
    role: String,
    replica_of: Option<(String, String)>, // host and port of master;
    master_replid: String,
    master_repl_offset: i64,
}

impl ServerInfo {
    pub fn new(args: ServerArguments) -> Self {
        match args.replica_of {
            Some(_) => {
                ServerInfo {
                    role: "slave".to_string(),
                    replica_of: args.replica_of,
                    master_replid: "?".to_string(),
                    master_repl_offset: -1,
                }
            },
            None => {
                ServerInfo {
                    role: "master".to_string(),
                    replica_of: None,
                    // this will be generated eventually...
                    master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                    master_repl_offset: 0,
                }
            },
        }
    }

    pub fn get_role(&self) -> String {
        self.role.clone()
    }

    pub fn get_master_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub fn get_master_repl_offset(&self) -> i64 {
        self.master_repl_offset.clone()
    }

    pub fn get_master_addr(&self) -> String {
        match &self.replica_of {
            Some((host, port)) => format!("{}:{}", host, port),
            None => "".to_string(),
        }
    }
}


pub struct Replica {
    stream: TcpStream,
    addr: SocketAddr,
}

impl Replica {
    pub fn new(addr: SocketAddr, stream: TcpStream) -> Self {
        Replica { addr, stream }
    }
}

pub struct Offset {
    offset: usize,
}

impl Offset {
    pub fn new() -> Self {
        Offset { offset: 0 }
    }

    pub fn get(&self) -> usize {
        self.offset
    }

    pub fn inc(&mut self) {
        self.offset += 1;
    }
}

pub struct History {
    repls: Mutex<Vec<Replica>>,
    write_history: Mutex<Vec<Resp>>,
    offset: Mutex<Offset>,
}

impl History {
    pub fn new() -> Self {
        History {
            repls: Mutex::new(Vec::new()),
            write_history: Mutex::new(Vec::new()),
            offset: Mutex::new(Offset::new()),
        }
    }

    pub async fn add_replica(&self, addr: SocketAddr, stream: TcpStream) {
        self.repls.lock().await.push(Replica::new(addr, stream));
    }

    pub async fn add_write(&self, resp: Resp) {
        self.write_history.lock().await.push(resp);
    }

    // todo handle lock errors and write errors etc...
    pub async fn sync(&self) {
        // send write history to all replicas
        let mut offset = self.offset.lock().await;
        let history = self.write_history.lock().await;
        let repls = self.repls.lock().await;

        for replica in repls.iter() {
            // send write history to replica
            for resp in history[offset.get()..].iter() {
                // send resp to replica
                let mut stream = TcpStream::connect(replica.addr).await.unwrap();
                let mut buf = BytesMut::new();
                RespEncoder::encode_resp(resp, &mut buf);
                let _ = stream.write_all(&buf).await;
                stream.shutdown().await.unwrap();
            }

            println!("Synced with replica: {:?}", replica.addr);
        }

        offset.inc();
    }
}

pub struct RedisServer {
    pub host: String,
    pub port: u64,
    pub listener: TcpListener,
    pub database: Database,
    pub info: ServerInfo,
    pub history: History,
}

impl RedisServer {
    pub async fn bind(args: ServerArguments) -> io::Result<Self> {
        let host = args.host.clone();
        let port = args.port;
        let addr = format!("{}:{}", host, port);

        println!("Listening on: {}", addr);

        let listener = TcpListener::bind(addr).await?;
        let database = Database::new();
        let info = ServerInfo::new(args);
        let history = History::new();
        
        Ok(RedisServer { host, port, listener, database, info, history })
    }

    pub async fn add_replica(&self, addr: SocketAddr, stream: TcpStream) {
        self.history.add_replica(addr, stream).await;
    }

    pub async fn add_write(&self, resp: Resp) {
        self.history.add_write(resp).await;
    }

    pub async fn sync(&self) {
        self.history.sync().await;
    }
}

// client resp is expected to be a an array of bulk strings...
pub fn client_resp_to_string(resp: Resp) -> io::Result<String> {
    match resp {
        Resp::SimpleString(s) => Ok(s),
        Resp::BulkString(b) => { 
            let s = String::from_utf8(b).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Err utf8 parse error"))?;
            Ok(s) 
        },
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Err utf8 parse error")),
    }
}

// helper method for 
pub async fn write_simple_error(stream: &mut TcpStream, msg: &str) -> io::Result<()> {
    let mut buf = BytesMut::new();
    RespEncoder::encode_simple_error(msg, &mut buf);
    stream.write_all(&buf).await?;
    Ok(())
}

pub async fn write_nil(stream: &mut TcpStream) -> io::Result<()> {
    let mut buf = BytesMut::new();
    RespEncoder::encode_array_null(&mut buf);
    stream.write_all(&buf).await?;
    Ok(())
}

pub async fn write_simple_string(stream: &mut TcpStream, msg: &str) -> io::Result<()> {
    let mut buf = BytesMut::new();
    RespEncoder::encode_simple_string(msg, &mut buf);
    stream.write_all(&buf).await?;
    Ok(())
}

pub async fn write_nil_bulk_string(stream: &mut TcpStream) -> io::Result<()> {
    let mut buf = BytesMut::new();
    RespEncoder::encode_bulk_string_null(&mut buf);
    stream.write_all(&buf).await?;
    Ok(())
}

pub async fn write_bulk_string_array(stream: &mut TcpStream, msgs: &[Resp]) -> io::Result<()> {
    let mut buf = BytesMut::new();
    RespEncoder::encode_array(msgs, &mut buf);
    stream.write_all(&buf).await?;
    Ok(())
}

pub async fn read_and_parse(stream: &mut TcpStream, buf: &mut BytesMut) -> io::Result<Resp> {
    loop {
        let mut chunk = [0; 1024];

        let nbytes = stream.read(&mut chunk).await?;

        if nbytes == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "unable to parse client stream."))
        }

        buf.extend_from_slice(&chunk[..nbytes]);

        let mut parser = RespParser::new(buf.clone());

        if let Ok(cmd) = parser.parse() {
           return Ok(cmd)
        }
    }
}
    