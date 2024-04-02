// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener };
use std::io;
use crate::database::{ Database };
use crate::arguments::{ ServerArguments };
use crate::listener::{ Listener };
use crate::history::History;

#[derive(Debug, Clone)]
pub struct ServerInfo {
    role: String,
    master_replid: String,
    master_repl_offset: Option<i64>,
    master_host: Option<String>,
}

impl ServerInfo {
    pub fn new(master_host: Option<(String, String)>) -> Self {
        if master_host.is_some() {
            Self::replica(master_host.unwrap())
        } else {
            Self::master()
        }
    }

    pub fn master() -> Self {
        ServerInfo {
            role: "master".to_string(),
            // this will be generated eventually...
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: None,
            master_host: None
        }
    }

    pub fn replica(master_host: (String, String)) -> Self {
        ServerInfo {
            role: "slave".to_string(),
            master_replid: "?".to_string(),
            master_repl_offset: Some(-1),
            master_host: Some(format!("{}:{}", master_host.0, master_host.1))
        }
    }

    pub fn get_role(&self) -> String {
        self.role.clone()
    }

    pub fn get_master_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub fn get_master_repl_offset(&self) -> Option<i64> {
        self.master_repl_offset.clone()
    }

    pub fn is_replica(&self) -> bool {
        if self.role == "master" { true } else { false }
    }
}

pub struct RedisServer {
    pub listener: Listener,
}

impl RedisServer {
    pub async fn bind(args: ServerArguments) -> io::Result<Self> {
        let addr = format!("{}:{}", args.host, args.port);
        let database = Database::new(); 
        let history = History::new();
        let info = ServerInfo::new(args.replica_of);
        
        let tcp_socket = TcpListener::bind(addr.clone()).await?;
        println!("Listening on: {}", addr);

        let listener = Listener::new(tcp_socket, database, history, info);
        Ok(RedisServer { listener })
    }
}

// client resp is expected to be a an array of bulk strings...
// pub fn client_resp_to_string(resp: Resp) -> io::Result<String> {
//     match resp {
//         Resp::SimpleString(s) => Ok(s),
//         Resp::BulkString(b) => { 
//             let s = String::from_utf8(b).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Err utf8 parse error"))?;
//             Ok(s) 
//         },
//         _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Err utf8 parse error")),
//     }
// }

// // helper method for 
// pub async fn write_simple_error(stream: &mut TcpStream, msg: &str) -> io::Result<()> {
//     let mut buf = BytesMut::new();
//     RespEncoder::encode_simple_error(msg, &mut buf);
//     stream.write_all(&buf).await?;
//     Ok(())
// }

// pub async fn write_nil(stream: &mut TcpStream) -> io::Result<()> {
//     let mut buf = BytesMut::new();
//     RespEncoder::encode_array_null(&mut buf);
//     stream.write_all(&buf).await?;
//     Ok(())
// }

// pub async fn write_simple_string(stream: &mut TcpStream, msg: &str) -> io::Result<()> {
//     let mut buf = BytesMut::new();
//     RespEncoder::encode_simple_string(msg, &mut buf);
//     stream.write_all(&buf).await?;
//     Ok(())
// }

// pub async fn write_nil_bulk_string(stream: &mut TcpStream) -> io::Result<()> {
//     let mut buf = BytesMut::new();
//     RespEncoder::encode_bulk_string_null(&mut buf);
//     stream.write_all(&buf).await?;
//     Ok(())
// }

// pub async fn write_bulk_string_array(stream: &mut TcpStream, msgs: &[Resp]) -> io::Result<()> {
//     let mut buf = BytesMut::new();
//     RespEncoder::encode_array(msgs, &mut buf);
//     stream.write_all(&buf).await?;
//     Ok(())
// }

    