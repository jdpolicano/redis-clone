use tokio::io::AsyncWriteExt;
use bytes::BytesMut;
use crate::database::{ Record };
use crate::resp::{Resp, RespEncoder};
use crate::server::{ write_simple_error, write_nil, write_nil_bulk_string, write_simple_string };
use crate::context::Context;
use crate::arguments::{ SetArguments, EchoArguments, GetArguments };

// Command trait to represent any executable command.
// Commands must be no
pub trait Command {
    fn execute(self, ctx: &mut Context) -> impl std::future::Future<Output = ()> + Send;
}

// Implement the Command trait for different commands
pub struct PingCommand;
pub struct EchoCommand(EchoArguments);
pub struct SetCommand(SetArguments);
pub struct GetCommand(GetArguments);

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
    pub fn new(args: EchoArguments) -> Self {
        EchoCommand(args)
    }
}

impl Command for EchoCommand {
    async fn execute(self, ctx: &mut Context) {
        let mut buf = BytesMut::new();
        RespEncoder::encode_resp(&self.0.message, &mut buf);
        let res = ctx.stream.write_all(&buf).await;
        if let Err(e) = res {
            ctx.log(&format!("Error writing to stream: {}", e));
        }
    }
}

impl SetCommand {
  pub fn new(args: SetArguments) -> Self {
    SetCommand(args)
  }
}

impl Command for SetCommand {
    async fn execute(mut self, ctx: &mut Context) {
        let args = self.0;
        let key = args.key;
        let mut value = args.value;
        let expiration = args.expiration;

        if let Some(mut expiration) = expiration {
            value.set_expiry(expiration.as_duration());
        }
    
        if args.nx && args.xx {
            let msg = "ERR syntax error: nx and xx options are mutually exclusive";
            let _ = write_simple_error(&mut ctx.stream, msg).await;
            return;
        }

        if args.nx {
            if ctx.db.exists(&key) {
                let _  = write_nil(&mut ctx.stream).await;
                return;
            }

            ctx.db.set(key, value);
            // this is a conflict - cant get the previous key if we just 
            // set it for the first time.
            if args.get {
                let _ = write_nil(&mut ctx.stream).await;
                return;
            }

            let _ = write_simple_string(&mut ctx.stream, "OK").await;
            return;
        }

        if args.xx {
            if !ctx.db.exists(&key) {
                let _ = write_nil(&mut ctx.stream).await;
                return;
            }

            let prev = ctx.db.set(key, value);

            if args.get {
                if let Some(prev) = prev {
                    let mut buf = BytesMut::new();
                    RespEncoder::encode_bulk_string(&prev.data, &mut buf);
                    let _ = ctx.stream.write_all(&buf).await;
                    return;
                }

                // key didn't exist, so we return nil
                let _ = write_nil(&mut ctx.stream).await;
                return;
            }

            let _ = write_simple_string(&mut ctx.stream, "OK").await;
            return;
        }

        // if we get here, we're just setting the key
        
        let prev = ctx.db.set(key, value);

        if args.get {
            if let Some(value) = prev {
                let mut buf = BytesMut::new();
                RespEncoder::encode_bulk_string(&value.data, &mut buf);
                let _ = ctx.stream.write_all(&buf).await;
                return;
            }

            let _ = write_nil_bulk_string(&mut ctx.stream).await;
            return;
        }

        let _ = write_simple_string(&mut ctx.stream, "OK").await;
    }
}


impl GetCommand {
    pub fn new(args: GetArguments) -> Self {
        GetCommand(args)
    }
}

impl Command for GetCommand {
    async fn execute(self, ctx: &mut Context) {

        let key = self.0.key;
        let value = ctx.db.get(&key);

        if value.is_none() {
          let mut buf = BytesMut::new();
          RespEncoder::encode_bulk_string_null(&mut buf);
          if let Err(e) = ctx.stream.write_all(&buf).await {
            ctx.log(&format!("{}", e));
          }
          return;
        }

        let payload = value.unwrap();

        if payload.has_expired() {
          ctx.db.del(&key);
          let mut buf = BytesMut::new();
          RespEncoder::encode_bulk_string_null(&mut buf);
          if let Err(e) = ctx.stream.write_all(&buf).await {
            ctx.log(&format!("Write to stream failed {}", e));
          }
        }

        let mut buf = BytesMut::new();
        RespEncoder::encode_bulk_string(&payload.data, &mut buf);
        if let Err(e) = ctx.stream.write_all(&buf).await {
            ctx.log(&format!("Write to stream failed {}", e));
        }
    }
}