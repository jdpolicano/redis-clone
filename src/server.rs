// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use std::io::{ self };
use crate::resp::{ RespParser, Resp, RespEncoder};
use crate::database::{ Database, DbType };
use crate::command::{ PingCommand, EchoCommand, SetCommand, Command };
use crate::context::Context;
use std::sync::Arc;

struct RedisServer {
    listener: TcpListener,
    database: Arc<Database>,
}

impl RedisServer {
    async fn bind(addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let database = Arc::new(Database::new());
        Ok(RedisServer { listener, database })
    }

    async fn run(&mut self) {
        loop {
            let (stream, addr) = self.listener.accept().await.unwrap();
            let db = self.database.clone();
            tokio::spawn(async move {
                let ctx = Context::new(db, stream, addr);
                let _ = self.handle_stream(ctx).await;
            });
        }
    }

    async fn handle_stream(&self, mut ctx: Context) -> io::Result<()> {
        let mut buffer = BytesMut::new();

        loop {
            let mut chunk = [0; 1024];
            let nbytes = stream.read(&mut chunk).await?;
    
            if nbytes == 0 {
                return Ok(());
            }
    
            buffer.extend_from_slice(&chunk[..nbytes]);
    
            let mut parser = RespParser::new(buffer.clone());
    
            if let Ok(cmd) = parser.parse() {
                ctx.log(&format!("Parsed: {:?}", cmd));
                match self.handle_command(&mut stream, cmd, ctx).await {
                    Ok(_) => {
                        buffer.clear();
                    }
                    Err(e) => {
                        ctx.log(&format!("Error: {}", e));
                        buffer.clear();
                    }
                }
            } else {
                ctx.log("Failed to parse");
            }
        }
    }

    async fn handle_command(&self, cmd: Resp, ctx: Context) -> io::Result<()> {
        if let Resp::Array(a) = cmd {
            Resp::Array(a) => {
                if a.len() == 0 {
                    ctx.log("Empty command");
                    self.write_error(&mut ctx.stream, "ERR empty command").await;
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Empty command"));
                }

                let cmd = self.client_resp_to_string(a[0]);

                if cmd.is_err() {
                    ctx.log("Invalid command");
                    self.write_error(&mut ctx.stream, "ERR invalid command").await;
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid command"));
                }

                match cmd.unwrap().to_uppercase().as_str() {
                    // "PING" => {
                    //     let ping = PingCommand::new();
                    //     ping.execute(ctx).await;
                    // }
                    // "ECHO" => {
                    //     let echo = EchoCommand::new(a.get(1).cloned());
                    //     echo.execute(ctx).await;
                    // }
                    // "SET" => {
                    //     if a.len() != 3 {
                    //         ctx.log("SET command requires 2 arguments");
                    //         return Ok(());
                    //     }
                    //     let key = a[1].as_str().unwrap().to_string().into_bytes();
                    //     let value = a[2].clone();
                    //     let set = SetCommand(key, value);
                    //     set.execute(ctx).await;
                    // }
                    // _ => {
                    //     ctx.log(&format!("Unknown command: {}", cmd));
                    // }
                    _ => { Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown command")) }
                }
            }
        }
    }

    // client resp is expected to be a an array of bulk strings...
    fn client_resp_to_string(&self, resp: Resp) -> io::Result<String> {
        match resp {
            Resp::SimpleString(s) => Ok(s),
            Resp::BulkString(b) => Ok(String::from_utf8(b).unwrap()),
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid response type")),
        }
    }

    async fn write_error(&self, stream: &mut TcpStream, msg: &str) {
        let mut buf = BytesMut::new();
        RespEncoder::encode_simple_error(msg, &mut buf);
        stream.write_all(&buf).await;
    }
}

    