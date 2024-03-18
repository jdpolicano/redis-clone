// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use redis_starter_rust::server::{ RedisServer, write_simple_error, client_resp_to_string };
use redis_starter_rust::resp::{ RespParser, Resp, RespEncoder};
use redis_starter_rust::database::{ Database, DbType };
use redis_starter_rust::context::Context;
use redis_starter_rust::command::{Command, PingCommand, EchoCommand, SetCommand, GetCommand};
use std::sync::Arc;


#[tokio::main]
async fn main() -> io::Result<()> {
    let mut server = RedisServer::new("127.0.0.1:6379").await?;
    loop {
        let (stream, addr) = server.listener.accept().await?;
        let db = server.database.clone();
        tokio::spawn(async move {
            let ctx = Context::new(db, stream, addr);
            let _ = handle_stream(ctx).await;
        });
    }
}

async fn handle_stream(mut ctx: Context) -> io::Result<()>  {
        let mut buffer = BytesMut::new();
        let mut success = false;

        loop {
            let mut chunk = [0; 1024];
            let nbytes = ctx.stream.read(&mut chunk).await?;
    
            if nbytes == 0 {
                ctx.log("client stream EOF");
                if !success {
                  ctx.log("unable to parse client message");
                }
                return Ok(());
            }
    
            buffer.extend_from_slice(&chunk[..nbytes]);
    
            let mut parser = RespParser::new(buffer.clone());
    
            if let Ok(cmd) = parser.parse() {
                success = true;
                ctx.log(&format!("Parsed: {:?}", cmd));
                match handle_command(cmd, &mut ctx).await {
                    Ok(_) => {
                        buffer.clear();
                    }
                    Err(e) => {
                        println!("{:?}", e);
                        let e_msg = &format!("Error: {}", e);
                        ctx.log(e_msg);
                        write_simple_error(&mut ctx.stream, e_msg).await?;
                        buffer.clear();
                    }
                }
            } else {
                ctx.log("partial stream, failed to parse");
            }
        }
}

async fn handle_command(cmd: Resp, ctx: &mut Context) -> io::Result<()> {
        match cmd {
            Resp::Array(mut a) => {
                if a.len() == 0 {
                    let msg = "ERR empty command";
                    ctx.log(msg);
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
                }

                // reverse the arguments so we can pop them out in the correct order..
                a.reverse();

                // this is okay because we confired array isn't empty already.
                let cmd_name = client_resp_to_string(a.pop().unwrap())?;

                match cmd_name.to_uppercase().as_str() {
                    "ECHO" => { return echo(a, ctx).await },
                    "PING" => { return ping(ctx).await },
                    "SET" => { return set(a, ctx).await },
                    "GET" => { return get(a, ctx).await },
                    _ => { return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown command")) }
                }
            },

              _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Err invalid client input, expected array of bulk strings..."))
          }
}

// args is still encoded as Resp at this point in time...
async fn echo(mut args: Vec<Resp>, ctx: &mut Context) -> io::Result<()> {
  let mut e = EchoCommand::new(args.pop());
  e.execute(ctx).await;
  Ok(())
}

async fn ping(ctx: &mut Context) -> io::Result<()> {
  let mut p = PingCommand::new();
  p.execute(ctx).await;
  Ok(())
}


async fn set(mut args: Vec<Resp>, ctx: &mut Context) -> io::Result<()> {
  let mut s = SetCommand::new(args);
  s.execute(ctx).await;
  Ok(())
}

async fn get(mut args: Vec<Resp>, ctx: &mut Context) -> io::Result<()> {
  let mut g = GetCommand::new(args.pop());
  g.execute(ctx).await;
  Ok(())
}
