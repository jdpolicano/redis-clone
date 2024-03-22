// Uncomment this block to pass the first stage
use tokio::io::{ AsyncWriteExt};
use tokio::net::{ TcpStream };
use bytes::BytesMut;
use std::io::{ self };
use std::vec::IntoIter;
use std::sync::Arc;
use redis_starter_rust::server::{ 
    RedisServer, 
    write_simple_error, 
    client_resp_to_string,
    read_and_parse,
};
use redis_starter_rust::resp::{ Resp, RespEncoder };
use redis_starter_rust::context::Context;
use redis_starter_rust::command::{Command, PingCommand, EchoCommand, SetCommand, GetCommand};
use redis_starter_rust::arguments::{ EchoArguments, SetArguments, GetArguments, ServerArguments };
use redis_starter_rust::client::RedisClient;

#[tokio::main]
async fn main() -> io::Result<()> {
    let server_args = ServerArguments::parse();
    let server = Arc::new(RedisServer::bind(server_args).await?);

    if server.info.get_role() != "master" {
        let remote = TcpStream::connect(server.info.get_master_addr()).await?;
        let addr = remote.peer_addr()?;
        let ctx = Context::new(server.clone(), remote, addr);
        // what happens if we start getting requests before this negoatiation is done?
        // will need to add some state to the server struct that tells it if it's ready to accept requests or not.
        tokio::spawn(async move {
            let replication = negotiate_replication(ctx).await;
            if let Err(e) = replication {
                eprintln!("Error negotiating replication: {}", e);
            }
        });
    }

    loop {
        let (stream, addr) = server.listener.accept().await?;
        let ctx = Context::new(server.clone(), stream, addr);
        tokio::spawn(async move {
            let _ = handle_stream(ctx).await;
        });
    }
}

async fn handle_stream(mut ctx: Context) -> io::Result<()>  {
    let mut buffer = BytesMut::new();

    loop {
        let cmd = read_and_parse(&mut ctx.stream, &mut buffer).await?;

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
                    "REPLCONF" => { 
                        // this is a replication command, we need to check if we're a master or slave
                        if ctx.server.info.get_role() == "master" {
                            // we're a master, we don't need to do anything with this command but send OK back
                            let mut buf = BytesMut::new();
                            RespEncoder::encode_simple_string("OK", &mut buf);
                            ctx.stream.write_all(&buf).await?;
                            Ok(())
                        } else {
                            // we're a slave, we need to forward this command to the master
                            // we need to check if we've already negotiated replication
                            // if we haven't, we need to wait until we have before we can forward this command
                            // if we have, we can forward this command to the master
                            return Err(io::Error::new(io::ErrorKind::InvalidInput, "ERR not yet implemented"));
                        }
                     }
                    _ => { return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown command")) }
                }
            },

              _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Err invalid client input, expected array of bulk strings..."))
          }
}

async fn negotiate_replication(mut ctx: Context) -> io::Result<()> {
    let mut client = RedisClient::from_stream(&mut ctx.stream);
    client.ping().await?;
    client.repl_conf(&["listening-port", &ctx.server.port.to_string()]).await?;
    client.repl_conf(&["capa", "psync2"]).await?;
    Ok(())
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
        "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}", 
        ctx.server.info.get_role(),
        ctx.server.info.get_master_replid(),
        ctx.server.info.get_master_repl_offset()
    );

    RespEncoder::encode_bulk_string(&payload.as_bytes(), &mut buf);
    ctx.stream.write_all(&buf).await?;
    Ok(())
}
