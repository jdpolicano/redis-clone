use std::sync::Arc;
use tokio::io::{self, AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use bytes::BytesMut;
use crate::database::Database;
use crate::resp::{Resp, RespParser, RespEncoder};
use crate::context::Context;

// Command trait to represent any executable command.
// commands must be no
pub trait Command {
    async fn execute(&self, ctx: Context);
}

// Implement the Command trait for different commands
pub struct PingCommand;
pub struct EchoCommand(Option<Resp>);
pub struct SetCommand(Vec<u8>, Resp);

impl PingCommand {
    pub fn new() -> Self {
        PingCommand
    }
}

impl Command for PingCommand {
    async fn execute(&self, mut ctx: Context) {
        let mut buf = BytesMut::new();
        RespEncoder::encode_simple_string("PONG", &mut buf);
        let res = ctx.stream.write_all(&buf).await;
        if let Err(e) = res {
            ctx.log(&format!("Error writing to stream: {}", e));
        }
    }
}

impl EchoCommand {
    pub fn new(resp: Option<Resp>) -> Self {
        EchoCommand(resp)
    }
}

impl Command for EchoCommand {
    async fn execute(&self, mut ctx: Context) {
        let mut buf = BytesMut::new();
        if let Some(resp) = &self.0 {
            RespEncoder::encode_resp(resp, &mut buf);
            let res = ctx.stream.write_all(&buf).await;
            if let Err(e) = res {
                ctx.log(&format!("Error writing to stream: {}", e));
            }
        } else {
            ctx.log("No echo value provided");
        }
    }
}

// impl Command for SetCommand {
//     fn execute(&self, db: Arc<Database>, stream: &mut TcpStream) -> CommandResult {
//         db.set(self.0.clone().into_bytes(), self.1.clone());
//         let mut buf = BytesMut::new();
//         RespEncoder::encode_simple_string("OK", &mut buf);
//         stream.write_all(&buf).await
//     }
// }