use tokio::io::AsyncWriteExt;
use bytes::BytesMut;
use crate::database::{ Record };
use crate::resp::{Resp, RespEncoder};
use crate::server::{ write_simple_error };
use crate::context::Context;

// Command trait to represent any executable command.
// commands must be no
pub trait Command {
    fn execute(self, ctx: &mut Context) -> impl std::future::Future<Output = ()> + Send;
}

// Implement the Command trait for different commands
pub struct PingCommand;
pub struct EchoCommand(Option<Resp>);
pub struct SetCommand(Vec<Resp>);
pub struct GetCommand(Option<Resp>);

impl PingCommand {
    pub fn new() -> Self {
        PingCommand
    }
}

impl Command for PingCommand {
    async fn execute(self, ctx: &mut Context) {
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
    async fn execute(self, ctx: &mut Context) {
        let mut buf = BytesMut::new();
        if let Some(resp) = &self.0 {
            RespEncoder::encode_resp(resp, &mut buf);
            let res = ctx.stream.write_all(&buf).await;
            if let Err(e) = res {
                ctx.log(&format!("Error writing to stream: {}", e));
            }
        } else {
            let msg = "ERR no value to echo";
            ctx.log(msg);
            let _ = write_simple_error(&mut ctx.stream, msg).await;
        }
    }
}

impl SetCommand {
  pub fn new(args: Vec<Resp>) -> Self {
    SetCommand(args)
  }

  async fn get_kv(&mut self, ctx: &mut Context) -> (Record, Record) {
        let key  = self.0.pop();
        let value = self.0.pop();
        if key.is_none() || value.is_none() {
            let e_msg = "Err SET requires key and value";
            ctx.log(e_msg);
            let _ = write_simple_error(&mut ctx.stream, e_msg).await;
            return (Record::None, Record::None);
        }

        let k = Record::from_resp(key.unwrap());
        let v = Record::from_resp(value.unwrap());

        if k.is_none() || v.is_none() {
            let e_msg = "Err SET key/value isn't bulk string";
            ctx.log(e_msg);
            let _ = write_simple_error(&mut ctx.stream, e_msg).await;
            return (Record::None, Record::None);
        }

        (k.unwrap(), v.unwrap())
  }

  async fn get_args(&mut self, ctx: &mut Context) -> (Record, Record) {
    let key  = self.0.pop();
    let value = self.0.pop();
    if key.is_none() || value.is_none() {
        let e_msg = "Err SET requires key and value";
        ctx.log(e_msg);
        let _ = write_simple_error(&mut ctx.stream, e_msg).await;
        return (Record::None, Record::None);
    }

    let k = Record::from_resp(key.unwrap());
    let v = Record::from_resp(value.unwrap());

    if k.is_none() || v.is_none() {
        let e_msg = "Err SET key/value isn't bulk string";
        ctx.log(e_msg);
        let _ = write_simple_error(&mut ctx.stream, e_msg).await;
        return (Record::None, Record::None);
    }

    (k.unwrap(), v.unwrap())
  }
}

impl Command for SetCommand {
    async fn execute(mut self, ctx: &mut Context) {
        let (key, value) = self.get_kv(ctx).await;
        
        if key.is_string() && !value.is_none() {
            ctx.db.set(key, value);
            let mut buf = BytesMut::new();
            RespEncoder::encode_simple_string("OK", &mut buf);
            let _ = ctx.stream.write_all(&buf).await;
        }
    }
}


impl GetCommand {
    pub fn new(resp: Option<Resp>) -> Self {
        GetCommand(resp)
    }
}

impl Command for GetCommand {
    async fn execute(self, ctx: &mut Context) {
        if self.0.is_none() {
            let msg = "Err no key specified";
            ctx.log(msg);
            let _ = write_simple_error(&mut ctx.stream, msg).await;
            return;
        }

        let key = Record::from_resp(self.0.unwrap());

        if key.is_none() {
          let msg = "Err key malformed, expected bulk string";
          ctx.log(msg);
          let _ = write_simple_error(&mut ctx.stream, msg).await;
          return;
        }

        let value = ctx.db.get(&key.unwrap());

        if value.is_none() {
          let mut buf = BytesMut::new();
          RespEncoder::encode_bulk_string_null(&mut buf);
          if let Err(e) = ctx.stream.write_all(&buf).await {
            ctx.log(&format!("{}", e));
          }
          return;
        }

        let payload = value.unwrap();

        if !payload.is_string() {
            ctx.log("ENOTSUPPORTED: lists, maps, etc...");
            return;
        }

        let mut buf = BytesMut::new();
        RespEncoder::encode_bulk_string(&payload.to_bytes().unwrap(), &mut buf);
        if let Err(_) = ctx.stream.write_all(&buf).await {
            ctx.log("EWRITEERR: write to client stream failed...");
        }
    }
}