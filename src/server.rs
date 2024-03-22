// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncWriteExt, AsyncReadExt };
use bytes::BytesMut;
use std::io::{ self };
use crate::resp::{ Resp, RespParser, RespEncoder};
use crate::database::{ Database };
use crate::arguments::{ ServerArguments };

pub struct ServerInfo {
    role: String,
    replica_of: Option<(String, u64)>,
    master_replid: String,
    master_repl_offset: u64,
}

impl ServerInfo {
    pub fn new(args: ServerArguments) -> Self {
        let role = match args.replica_of {
            Some(_) => "slave".to_string(),
            None => "master".to_string(),
        };

        let replica_of = args.replica_of;
        // this should use a num generatore in the future...
        let rand_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();

        ServerInfo { 
            role, 
            replica_of,
            master_replid: rand_id,
            master_repl_offset: 0,
        }
    }

    pub fn get_role(&self) -> String {
        self.role.clone()
    }

    pub fn get_master_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub fn get_master_repl_offset(&self) -> u64 {
        self.master_repl_offset.clone()
    }

    pub fn get_replica_of(&self) -> (String, u64) {
        match &self.replica_of {
            Some((host, port)) => (host.clone(), *port),
            None => ("".to_string(), 0),
        }
    }

    pub fn get_master_addr(&self) -> String {
        match &self.replica_of {
            Some((host, port)) => format!("{}:{}", host, port),
            None => "".to_string(),
        }
    }
}


pub struct RedisServer {
    pub host: String,
    pub port: u64,
    pub listener: TcpListener,
    pub database: Database,
    pub info: ServerInfo
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
        
        Ok(RedisServer { host, port, listener, database, info })
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
    