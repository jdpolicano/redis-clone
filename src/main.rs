// Uncomment this block to pass the first stage
use std::io::{ self };
use redis_starter_rust::server::{ RedisServer };
use redis_starter_rust::arguments::{ ServerArguments };

#[tokio::main]
async fn main() -> io::Result<()> {
    let server_args = ServerArguments::parse();
    let server = RedisServer::bind(server_args).await?;
    server.listener.run().await?;
    Ok(())
}

// async fn handle_stream(ctx: &mut Context) -> io::Result<()>  {
//     loop {
//         let cmd = ctx.stream.read_message().await?;

//         println!("cmd: {:?}", cmd);

//         if let Err(e) = handle_command(cmd, ctx).await {
//             println!("Error: {}", e);
//             ctx.stream.write_message(&Resp::SimpleError(e.to_string()));
//         }

//         if ctx.keep_connection_alive {
//             return Ok(());
//         }
//     }
// }


// async fn handle_command(cmd: Resp, ctx: &mut Context) -> io::Result<()> {
//         match cmd {
//             Resp::Array(a) => {
//                 if a.len() == 0 {
//                     return Err(io::Error::new(io::ErrorKind::InvalidInput, "ERR empty command"));
//                 }

//                 let a_copy = a.clone(); // we will need this to propagate the command to the replica later.
//                 let mut args_iter = a.into_iter();

//                 // this is okay because we confired array isn't empty already.
//                 let cmd_name = client_resp_to_string(args_iter.next().unwrap())?;

//                 println!("recieved command: {}", cmd_name.to_uppercase());
//                 println!("isreplica?: {}", ctx.server.is_replica());

//                 match cmd_name.to_uppercase().as_str() {
//                     "ECHO" => { return echo(args_iter, ctx).await },
//                     "PING" => { return ping(ctx).await },
//                     "SET" => {
//                         let res = set(args_iter, ctx).await;

//                         if ctx.server.get_role() == "master" {
//                             ctx.server.add_write(Resp::Array(a_copy));
//                             ctx.server.sync().await;
//                             println!("finished syncing...");
//                         }

//                         return res
//                     },
//                     "GET" => { return get(args_iter, ctx).await },
//                     "INFO" => { return info(args_iter, ctx).await },
//                     "PSYNC" => { return psync(args_iter, ctx).await },
//                     "REPLCONF" => { 
//                         // this is a replication command, we need to check if we're a master or slave
//                         if ctx.server.get_role() == "master" {
//                             // we're a master, we don't need to do anything with this command but send OK back
//                             return repl_conf(args_iter, ctx).await;
//                         } else {
//                             return Err(io::Error::new(io::ErrorKind::InvalidInput, "ERR not yet implemented"));
//                         }
//                      }
//                     _ => { return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown command")) }
//                 }
//             },

//               _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Err invalid client input, expected array of bulk strings..."))
//           }
// }

// async fn negotiate_replication(ctx: &mut Context) -> io::Result<()> {
//     let mut client = RedisClient::from_stream(&mut ctx.stream);

//     client.ping().await?;
//     // yikes...
//     client.repl_conf(&["listening-port", &ctx.server.listener.local_addr()?.port().to_string()]).await?;
//     client.repl_conf(&["capa", "psync2"]).await?;

//     let master_replid = ctx.server
//         .get_master_replid();
//     let master_repl_offset = ctx.server
//         .get_master_repl_offset()
//         .to_string();

//     client.psync(&[&master_replid, &master_repl_offset]).await?;

//     println!("synced with master...");

//     Ok(())
// }

// // args is still encoded as Resp at this point in time...
// async fn echo(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
//     let parsed_args = EchoArguments::parse(args);

//     // check if the arguments are valid, and return an io error otherwise...
//     if let Err(e) = parsed_args {
//         return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
//     }

//     let e = EchoCommand::new(parsed_args.unwrap());
//     e.execute(ctx).await;
//     Ok(())
// }

// async fn ping(ctx: &mut Context) -> io::Result<()> {
//   let p = PingCommand::new();
//   p.execute(ctx).await;
//   Ok(())
// }


// async fn set(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
//     let parsed_args = SetArguments::parse(args);

//     // check if the arguments are valid, and return an io error otherwise...
//     if let Err(e) = parsed_args {
//         return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
//     }
    
//     let s = SetCommand::new(parsed_args.unwrap());

//     s.execute(ctx).await;

//     Ok(())
// }

// async fn get(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
//     let parsed_args = GetArguments::parse(args);

//     // check if the arguments are valid, and return an io error otherwise...
//     if let Err(e) = parsed_args {
//         return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
//     }

//     let g = GetCommand::new(parsed_args.unwrap());
//     g.execute(ctx).await;
//     Ok(())
// }

// async fn repl_conf(args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
//     let parsed_args = ReplconfArguments::parse(args);

//     if let Err(e) = parsed_args {
//         return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
//     }

//     let r = ReplconfCommand::new(parsed_args.unwrap());
//     r.execute(ctx).await;
//     Ok(())
// }

// async fn info(_args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
//     let mut buf = BytesMut::new();

//     let payload = format!(
//         "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}", 
//         ctx.server.get_role(),
//         ctx.server.get_master_replid(),
//         ctx.server.get_master_repl_offset()
//     );

//     ctx.stream.write_bytes(payload.as_bytes()).await;
//     Ok(())
// }

// async fn psync(_args: IntoIter<Resp>, ctx: &mut Context) -> io::Result<()> {
//     let repl_id = ctx.server.get_master_replid();
//     let repl_offset = ctx.server.get_master_repl_offset();
//     let payload = format!("FULLRESYNC {} {}", repl_id, repl_offset);

//     ctx.stream.write_bytes(payload.as_bytes()).await;

//     let rdb_file = get_empty_rdb_file();
//     ctx.stream.write(format!("${}\r\n", rdb_file.len()).as_bytes());
//     ctx.stream.write(&rdb_file);
//     ctx.stream.flush().await;
//     ctx.keep_alive();

//     // store the replica in the history...
//     Ok(())
// }


