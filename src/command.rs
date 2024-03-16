// use std::sync::Arc;
// use tokio::io::{self, AsyncWriteExt, AsyncReadExt};
// use tokio::net::TcpStream;
// use bytes::BytesMut;
// use redis_starter_rust::database::Database;
// use redis_starter_rust::resp::{Resp, RespParser, RespEncoder};

// // Command trait to represent any executable command
// pub trait Command {
//     fn execute(&self, db: Arc<Database>, stream: &mut TcpStream) -> io::Result<()>;
// }

// // Implement the Command trait for different commands
// struct PingCommand;
// struct EchoCommand(Resp);
// struct SetCommand(Vec<u8>, Resp);

// // returns a resp enum that can be encoded and sent directly to the client if needed. 
// pub type CommandResult = Result<Resp, String>;

// impl Command for PingCommand {
//     fn execute(&self, db: Arc<Database>) -> CommandResult {
//         let mut buf = BytesMut::new();
//         RespEncoder::encode_simple_string("PONG", &mut buf);
//         stream.write_all(&buf).await
//     }
// }

// impl Command for EchoCommand {
//     fn execute(&self, db: Arc<Database>, stream: &mut TcpStream) -> CommandResult {
//         let mut buf = BytesMut::new();
//         RespEncoder::encode_resp(&self.0, &mut buf);
//         stream.write_all(&buf).await
//     }
// }

// impl Command for SetCommand {
//     fn execute(&self, db: Arc<Database>, stream: &mut TcpStream) -> CommandResult {
//         db.set(self.0.clone().into_bytes(), self.1.clone());
//         let mut buf = BytesMut::new();
//         RespEncoder::encode_simple_string("OK", &mut buf);
//         stream.write_all(&buf).await
//     }
// }