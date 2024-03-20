// Uncomment this block to pass the first stage

use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use std::vec::IntoIter;
use redis_starter_rust::server::{ RedisServer, write_simple_error, client_resp_to_string };
use redis_starter_rust::resp::{ RespParser, Resp, RespEncoder };
use redis_starter_rust::context::Context;
use redis_starter_rust::command::{Command, PingCommand, EchoCommand, SetCommand, GetCommand};
use redis_starter_rust::arguments::{ EchoArguments, SetArguments, GetArguments, ServerArguments };

#[tokio::main]
async fn main() -> io::Result<()> {
    let server_args = ServerArguments::parse();
    let server = RedisServer::new(server_args).await?;
    
    loop {
        let (stream, addr) = server.listener.accept().await?;
        let db = server.database.clone();
        let info = server.info.clone();

        tokio::spawn(async move {
            let ctx = Context::new(db, info, stream, addr);
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
            Resp::Array(a) => {
                if a.len() == 0 {
                    let msg = "ERR empty command";
                    ctx.log(msg);
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
                }

                // reverse the arguments so we can pop them out in the correct order..
                let mut args_iter = a.into_iter();

                // this is okay because we confired array isn't empty already.
                let cmd_name = client_resp_to_string(args_iter.next().unwrap())?;

                match cmd_name.to_uppercase().as_str() {
                    "ECHO" => { return echo(args_iter, ctx).await },
                    "PING" => { return ping(ctx).await },
                    "SET" => { return set(args_iter, ctx).await },
                    "GET" => { return get(args_iter, ctx).await },
                    "INFO" => { return info(args_iter, ctx).await },
                    _ => { return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown command")) }
                }
            },

              _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Err invalid client input, expected array of bulk strings..."))
          }
}

// args is still encoded as Resp at this point in time...
async fn echo(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
    let parsed_args = EchoArguments::parse(args);

    // check if the arguments are valid, and return an io error otherwise...
    if let Err(e) = parsed_args {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
    }

    let e = EchoCommand::new(parsed_args.unwrap());
    e.execute(ctx).await;
    Ok(())
}

async fn ping(ctx: &mut Context) -> io::Result<()> {
  let p = PingCommand::new();
  p.execute(ctx).await;
  Ok(())
}


async fn set(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
    let parsed_args = SetArguments::parse(args);

    // check if the arguments are valid, and return an io error otherwise...
    if let Err(e) = parsed_args {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
    }
    
    let s = SetCommand::new(parsed_args.unwrap());
    s.execute(ctx).await;
    Ok(())
}

async fn get(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
    let parsed_args = GetArguments::parse(args);

    // check if the arguments are valid, and return an io error otherwise...
    if let Err(e) = parsed_args {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
    }

    let g = GetCommand::new(parsed_args.unwrap());
    g.execute(ctx).await;
    Ok(())
}

async fn info(_args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
    let mut buf = BytesMut::new();
    let payload = format!(
        "{}\r\n{}\r\n{}", 
        ctx.info.get_role(),
        ctx.info.get_master_replid(),
        ctx.info.get_master_repl_offset()
    );

    RespEncoder::encode_bulk_string(&payload.as_bytes(), &mut buf);
    ctx.stream.write_all(&buf).await?;
    Ok(())
}
