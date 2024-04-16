use crate::resp::Resp;
use crate::command::{ Command, Transaction };
use crate::arguments::Argument;
use crate::protocol::ReplServerProtocol;
use crate::context::Handle;
use crate::connection::Connection;
use crate::client::RedisClient;
use std::vec::IntoIter;

pub struct ReplconfCommand(pub ReplconfArguments);
// the internal repl conf command will route the request to the appropriate 
// protocol handler, from there execution of the command is handled elsewhere.
impl Command for ReplconfCommand {
    async fn execute(self, stream: &mut Connection, handle: Handle) -> Transaction {
        match self.0 {
            ReplconfArguments::ListeningPort(_port) => {
                let mut protocol = ReplServerProtocol::new(stream, handle);
                let _ = protocol.start().await;
                return Transaction::Replicate;
            },

            ReplconfArguments::GetAck(_ack) => {
                let mut client = RedisClient::from_stream(stream);
                let offset = handle.info
                    .get_master_repl_offset()
                    .to_string();

                let _ = client.repl_conf(&["ACK", &offset]).await;
                return Transaction::None;
            },

            _ => {
                let _ = stream.write_err("ERR unsupported protocol sequence").await;
                return Transaction::None;
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplconfArguments {
    ListeningPort(String),
    Capa(String),
    GetAck(String),
}

impl Argument for ReplconfArguments {
    fn parse(mut args: IntoIter<Resp>) -> Result<Self, String> {
        if let Some(arg) = args.next() {

            let as_string: Option<String> = arg
                .try_into()
                .ok();

            if let Some(s) = as_string {
                match s.to_uppercase().as_str() {
                    "LISTENING-PORT" => {
                        let outcome = ReplconfArguments::ListeningPort(try_string(&mut args)?);
                        return check_remaining(outcome, &mut args);
                    },

                    "CAPA" => {
                        let outcome = ReplconfArguments::Capa(try_string(&mut args)?);
                        return check_remaining(outcome, &mut args);
                    },

                    "GETACK" => {
                        let outcome = ReplconfArguments::GetAck(try_string(&mut args)?);
                        return check_remaining(outcome, &mut args);
                    },

                    _ => return Err("ERR unknown or unexpected argument".to_string()),
                }
            }
        }

        Err("ERR expected argument".to_string())
    }
}

#[derive(Debug)]
pub enum PsyncArguments {
    FullResync(String, String) // (repl_id, repl_offset)
}

impl Argument for PsyncArguments {
    fn parse(mut args: IntoIter<Resp>) -> Result<Self, String> {
        let repl_id = try_string(&mut args)?;
        let repl_offset = try_string(&mut args)?;
        check_remaining(PsyncArguments::FullResync(repl_id, repl_offset), &mut args)
    }
}


fn try_string(args: &mut IntoIter<Resp>) -> Result<String, String> {
    if let Some(r) = args.next() {
        r.try_into().map_err(|_| "ERR argument conversion failed".to_string())
    } else {
        Err("ERR expected argument".to_string())
    }
}

fn check_remaining<T>(outcome: T, args: &mut IntoIter<Resp>) -> Result<T, String> {
    if args.count() > 0 {
        return Err("ERR too many arguments".to_string());
    }

    Ok(outcome)
}