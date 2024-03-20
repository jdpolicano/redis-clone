// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use crate::resp::{ Resp, RespEncoder};
use crate::database::{ Database };
use crate::arguments::{ ServerArguments };
use std::sync::Arc;

pub struct ServerInfo {
    role: String,
    replica_of: Option<(String, u64)>,
    master_repl_id: String,
    master_repl_offset: u64,
}

impl ServerInfo {
    pub fn new(args: ServerArguments) -> Self {
        let role = match args.replica_of {
            Some((ref host, port)) => "slave".to_string(),
            None => "master".to_string(),
        };

        let replica_of = args.replica_of;
        // this should use a num generatore in the future...
        let rand_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();

        ServerInfo { 
            role, 
            replica_of,
            master_repl_id: rand_id,
            master_repl_offset: 0,
        }
    }

    pub fn get_role(&self) -> String {
        format!("role:{}", self.role)
    }

    pub fn get_master_repl_id(&self) -> String {
        format!("master_repl_id:{}", self.master_repl_id)
    }

    pub fn get_master_repl_offset(&self) -> String {
        format!("master_repl_offset:{}", self.master_repl_offset)
    }
}


pub struct RedisServer {
    pub listener: TcpListener,
    pub database: Arc<Database>,
    pub info: Arc<ServerInfo>
}

impl RedisServer {
    pub async fn new(args: ServerArguments) -> io::Result<Self> {
        let addr = format!("{}:{}", args.host, args.port);
        println!("Listening on: {}", addr);
        let listener = TcpListener::bind(addr).await?;
        let database = Arc::new(Database::new());
        let info = Arc::new(ServerInfo::new(args));
        Ok(RedisServer { listener, database, info })
    }
}

// client resp is expected to be a an array of bulk strings...
pub fn client_resp_to_string(resp: Resp) -> io::Result<String> {
    // ideally wouldn't clone, but these should generally be small strings...
    match resp {
        Resp::SimpleString(s) => Ok(s),
        Resp::BulkString(b) => Ok(String::from_utf8(b.clone()).unwrap()),
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
    