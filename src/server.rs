// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use crate::resp::{ Resp, RespEncoder};
use crate::database::{ Database };
use crate::arguments::{ ServerArguments };
use std::sync::Arc;

pub struct RedisServer {
    pub listener: TcpListener,
    pub database: Arc<Database>,
}

impl RedisServer {
    pub async fn new(args: ServerArguments) -> io::Result<Self> {
        let addr = format!("{}:{}", args.host, args.port);
        println!("Listening on: {}", addr);
        let listener = TcpListener::bind(addr).await?;
        let database = Arc::new(Database::new());
        Ok(RedisServer { listener, database })
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
    