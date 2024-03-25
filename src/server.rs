// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncWriteExt, AsyncReadExt };
use tokio::sync::{ Mutex };
use bytes::BytesMut;
use std::io::{ self };
use crate::resp::{ Resp, RespParser, RespEncoder};
use crate::database::{ Database };
use crate::arguments::{ ServerArguments };

pub struct ServerInfo {
    role: String,
    master_replid: String,
    master_repl_offset: i64,
}

impl ServerInfo {
    pub fn new() -> Self {
        ServerInfo {
            role: "master".to_string(),
            // this will be generated eventually...
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
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

    pub fn set_role(&mut self, role: String) {
        self.role = role;
    }

    pub fn set_master_repl_offset(&mut self, offset: i64) {
        self.master_repl_offset = offset;
    }
    
    pub fn set_master_replid(&mut self, replid: String) {
        self.master_replid = replid;
    }

    pub fn to_replica(&mut self) {
        self.set_role("slave".to_string());
        self.set_master_replid("?".to_string());
        self.set_master_repl_offset(-1);
    }
}

#[derive(Debug)]
pub struct Replica {
    pub stream: TcpStream,
}

impl Replica {
    pub fn new(stream: TcpStream) -> Self {
        Replica { stream }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct History {
    repls: Vec<Replica>,
    write_history: Vec<Resp>,
    offset: Offset,
}

impl History {
    pub fn new() -> Self {
        History {
            repls: Vec::new(),
            write_history: Vec::new(),
            offset: Offset::new(),
        }
    }

    pub fn add_replica(&mut self, stream: TcpStream) {
        self.repls.push(Replica::new(stream));
    }

    pub fn add_write(&mut self, resp: Resp) {
        self.write_history.push(resp);
    }

    // todo handle lock errors and write errors etc...
    pub async fn sync(&mut self) {
        // send write history to all replicas
        for replica in self.repls.iter_mut() {
            // send write history to replica
            for resp in self.write_history[self.offset.get()..].iter() {
                // send resp to replica
                let stream = &mut replica.stream;
                let mut buf = BytesMut::new();
                RespEncoder::encode_resp(resp, &mut buf);
                let _ = stream.write_all(&buf).await;
            }

            println!("Synced with replica: {:?}", replica);
        }

        self.offset.inc();
    }
}

pub struct RedisServer {
    pub host: String, // read only
    pub port: String, // read only
    pub listener: TcpListener, // read
    pub database: Database, // 
    pub info: ServerInfo, //
    pub history: Mutex<History>, // read / write
    pub arguments: ServerArguments, // read only
    pub is_replica: bool, // read/write at first, then becomes readonly...
}

impl RedisServer {
    pub async fn bind(args: ServerArguments) -> io::Result<Self> {
        let addr = format!("{}:{}", args.host, args.port);

        println!("Listening on: {}", addr);

        let listener = TcpListener::bind(addr).await?;
        let database = Database::new(); // interio is already locked so no need to manage this ourselves.
        let info = ServerInfo::new();
        let history = Mutex::new(History::new());
        
        Ok(RedisServer { 
            host: args.host.clone(), 
            port: args.port.clone(), 
            listener, 
            database, 
            info, 
            history,
            arguments: args,
            is_replica: false,
        })
    }

    pub async fn add_replica(&self, stream: TcpStream) {
        self.history.lock().await.add_replica(stream);
    }

    pub async fn add_write(&self, resp: Resp) {
        self.history.lock().await.add_write(resp);
    }

    pub async fn sync(&self) {
        self.history.lock().await.sync().await;
    }

    pub fn set_role(&mut self, role: String) {
        self.info.set_role(role);
    }

    pub fn set_master_repl_offset(&mut self, offset: i64) {
        self.info.set_master_repl_offset(offset);
    }

    pub fn set_master_replid(&mut self, replid: String) {
        self.info.set_master_replid(replid);
    }

    pub fn get_role(&self) -> String {
        self.info.get_role()
    }

    pub fn get_master_replid(&self) -> String {
        self.info.get_master_replid()
    }

    pub fn get_master_repl_offset(&self) -> i64 {
        self.info.get_master_repl_offset()
    }

    pub fn is_replica(&self) -> bool {
        self.is_replica
    }

    pub fn to_replica(&mut self) {
        self.is_replica = true;
        self.info.to_replica();
    }

    pub fn get_master_address(&self) -> Option<String> {
        if let Some((host, port)) = &self.arguments.replica_of {
            Some(format!("{}:{}", host, port))
        } else {
            None
        }
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
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "ERR connection closed"));
        }

        buf.extend_from_slice(&chunk[..nbytes]);

        let mut parser = RespParser::new(buf.clone());

        if let Ok(cmd) = parser.parse() {
           return Ok(cmd)
        }
    }
}
    