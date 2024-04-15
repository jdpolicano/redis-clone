use crate::resp::Resp;
use crate::connection::Connection;
use crate::context::Handle;
use crate::arguments::{ ArgumentParser, CommandArgument, EchoArguments, SetArguments, GetArguments };
use crate::internals::{ ReplconfCommand };
// Enum for transaction results, used to propogate certain actions upward to the context handler
// i.e., if we performed a write operation the handler needs to send the info out to replicas
// i.e., if we performed a replication, we need to store the connection in the history and break.
pub enum Transaction {
    Write,
    Replicate,
    Read,
    None,
}

// Command trait to represent any executable command.
pub trait Command {
    fn execute(self, stream: &mut Connection, handle: Handle) -> impl std::future::Future<Output = Transaction> + Send;
}

// List of commands
pub struct PingCommand;
pub struct InfoCommand;
pub struct EchoCommand(EchoArguments);
pub struct SetCommand(SetArguments);
pub struct GetCommand(GetArguments);
pub struct PsyncCommand;

// Enum for each type to ease parsing into commands.
pub enum Cmd {
    Unexpected(String), // malformed command with err message...
    Ping(PingCommand),
    Echo(EchoCommand),
    Set(SetCommand),
    Get(GetCommand),
    Info(InfoCommand),
    ReplConf(ReplconfCommand),
}

impl Command for Cmd {
    async fn execute(self, stream: &mut Connection, handle: Handle) -> Transaction {
        match self {
            Cmd::Ping(c) => c.execute(stream, handle).await,
            Cmd::Echo(c) => c.execute(stream, handle).await,
            Cmd::Set(c) => c.execute(stream, handle).await,
            Cmd::Get(c) => c.execute(stream, handle).await,
            Cmd::Info(c) => c.execute(stream, handle).await,
            Cmd::ReplConf(c) => { c.execute(stream, handle).await },
            _ => Transaction::None
        }
    }
}

impl Command for PingCommand {
    async fn execute(self, stream: &mut Connection, _handle: Handle) -> Transaction {
        let _ = stream.write_str("PONG").await;
        Transaction::None
    }
}

impl Command for InfoCommand {
    async fn execute(self, stream: &mut Connection, handle: Handle) -> Transaction {
        let payload = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}", 
            handle.info.get_role(),
            handle.info.get_master_replid(),
            handle.info.get_master_repl_offset()
        );
        let _ = stream.write_bytes(&payload.as_bytes()).await;
        Transaction::None
    }
}


impl Command for EchoCommand {
    async fn execute(self, stream: &mut Connection, _handle: Handle) -> Transaction {
        let _ = stream.write_message(&self.0.message).await;
        Transaction::None
    }
}


impl Command for SetCommand {
    async fn execute(self, stream: &mut Connection, handle: Handle) -> Transaction {
        let args = self.0;

        let key = args.key;
        let mut value = args.value;
        let expiration = args.expiration;
    
        if let Some(expiration) = expiration {
            value.set_expiry(expiration.as_duration());
        }

        if args.nx {
            if handle.database.exists(&key) {
                let _ = stream.write_message(&Resp::BulkStringNull).await;
                return Transaction::None;
            }
        
            handle.database.set(key, value);
            // this is a conflict - cant get the previous key if we just set it.
            if args.get  {
                let _ = stream.write_message(&Resp::BulkStringNull).await;
                return Transaction::Write;
            }

            let _ = stream.write_str("OK").await;
            return Transaction::Write;
        }

        if args.xx {
            if !handle.database.exists(&key) {
                let _ = stream.write_message(&Resp::BulkStringNull).await;
                return Transaction::None;
            }

            let prev = handle.database.set(key, value);

            if args.get {
                if let Some(prev) = prev {
                    let _ = stream.write_bytes(&prev.data).await;
                    return Transaction::Write;
                }
                
                let _ = stream.write_message(&Resp::BulkStringNull).await;
                return Transaction::Write;
            }


            let _ = stream.write_str("OK").await;
            return Transaction::Write;
        }

        // if we get here, we're just setting the key
        let prev = handle.database.set(key, value);

        if args.get {
            if let Some(value) = prev {
                let _ = stream.write_bytes(&value.data);
                return Transaction::Write;
            }
            let _ = stream.write_message(&Resp::BulkStringNull).await;
            return Transaction::Write;
        }

        let _ = stream.write_str("OK").await;
        Transaction::Write
    }
}

impl Command for GetCommand {
    async fn execute(self, stream: &mut Connection, handle: Handle) -> Transaction {
        let key = self.0.key;
        let value = handle.database.get(&key);

        if value.is_none() {
            let _ = stream.write_message(&Resp::BulkStringNull).await;
            return Transaction::None;
        }

        let payload = value.unwrap();

        if payload.has_expired() {
            handle.database.del(&key);
            let _ = stream.write_message(&Resp::BulkStringNull).await;
            return Transaction::Write;
        }

        let _ = stream.write_bytes(&payload.data).await;
        Transaction::Read 
    }
}


impl Command for PsyncCommand {
    async fn execute(self, stream: &mut Connection, handle: Handle) -> Transaction {
        let repl_id = handle.info.get_master_replid();
        let repl_offset = handle.info.get_master_repl_offset();
        let payload = format!("FULLRESYNC {} {}", repl_id, repl_offset);

        let _ = stream.write_bytes(payload.as_bytes()).await;

        let rdb_file = get_empty_rdb_file();
        stream.write(format!("${}\r\n", rdb_file.len()).as_bytes());
        stream.write(&rdb_file);

        let _ = stream.flush().await;
        Transaction::Replicate
    }
}

pub struct CmdParser;

impl CmdParser {
    pub fn parse(input: Resp) -> Cmd {
        match input {
            Resp::Array(args) => {
                let args_iter = args.into_iter();
                return Self::route_cmd(args_iter);
            },

            _ => return Cmd::Unexpected("expected array of args".to_string())
        }
    }

    fn route_cmd(args: std::vec::IntoIter<Resp>) -> Cmd {
        let command_arg = ArgumentParser::get_from(args);

        if command_arg.is_err() {
            return Cmd::Unexpected(command_arg.unwrap_err());
        }

        match command_arg.unwrap() {
            CommandArgument::Ping => { 
                Cmd::Ping(PingCommand) 
            },

            CommandArgument::Info => {
                Cmd::Info(InfoCommand)
            },

            CommandArgument::Echo(echo_args) => {
                Cmd::Echo(EchoCommand(echo_args))
            },

            CommandArgument::Get(_get_args) => {
                Cmd::Get(GetCommand(_get_args))
            },

            CommandArgument::Set(_set_args) => {
                Cmd::Set(SetCommand(_set_args))
            },

            CommandArgument::Replconf(replconf_args) => {
                Cmd::ReplConf(ReplconfCommand(replconf_args))
            }

            _ => Cmd::Unexpected("unknown or unexpected command".to_string())
        }
    }
}


// TEMPORARY, REMOVE THIS AFTER RDB FILES ARE IMPLEMENTED
fn get_empty_rdb_file() -> Vec<u8> {
    let file_hex = b"524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let mut res = Vec::with_capacity(file_hex.len() / 2);

    fn decode(hex: u8) -> u8 {
        if hex as i32 - (b'a' as i32) < 0 {
            hex - b'0'
        } else {
            hex - b'a' + 10
        }
    }

    for chunk in file_hex.chunks(2) {

        if chunk.len() < 2 {
            panic!("Invalid hex string")
        }

        let upper = chunk[0];
        let lower = chunk[1];

        let decoded = (decode(upper) << 4) | decode(lower);
        res.push(decoded);
    }

    res
}