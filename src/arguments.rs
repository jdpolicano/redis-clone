use crate::resp::Resp;
use crate::database::Record;
use std::time::Duration;
use std::vec::IntoIter;
use crate::internals::{ReplconfArguments, PsyncArguments};

#[derive(Debug)]
pub enum CommandArgument {
    Ping,
    Info,
    Echo(EchoArguments),
    Get(GetArguments),
    Set(SetArguments),
    Replconf(ReplconfArguments),
    Psync(PsyncArguments),
}

// a trait defining an argument parser for a command
pub trait Argument {
    fn parse(args: IntoIter<Resp>) -> Result<Self, String>
    where
        Self: Sized;
}

#[derive(Debug)]
pub struct EchoArguments {
    pub message: Resp,
}

impl Argument for EchoArguments {
    fn parse(mut args: IntoIter<Resp>) -> Result<EchoArguments, String> {
        let message = match args.next() {
            Some(resp) => resp,
            _ => return Err("ERR wrong number of arguments for 'echo' command".to_string())
        };

        // Ensure the message is a bulk string; otherwise, return an error
        if let Resp::BulkString(_) = message {
            Ok(EchoArguments { message })
        } else {
            Err("ERR argument must be a bulk string".to_string())
        }
    }
}

#[derive(Debug)]
pub struct GetArguments {
    pub key: Vec<u8>,
}

impl Argument for GetArguments {
    fn parse(mut args: IntoIter<Resp>) -> Result<GetArguments, String> {
        // Since we check that there is exactly one argument, we can safely pop it
        let key = match args.next() {
            Some(Resp::BulkString(b)) => b,
            _ => return Err("ERR argument must be a bulk string".to_string()),
        };

        Ok(GetArguments { key })
    }
}

// this file defines bindings for parsing the arguments of the SET command in a Redis-like server
#[derive(Debug)]
pub struct SetArguments {
    pub key: Vec<u8>,
    pub value: Record,
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
    pub expiration: Option<Expiration>,
}

#[derive(Debug)]
pub enum Expiration {
    Seconds(u64),
    Milliseconds(u64),
}

impl Expiration {
    pub fn as_duration(&self) -> Duration {
        match self {
            Expiration::Seconds(s) => Duration::from_secs(*s),
            Expiration::Milliseconds(ms) => Duration::from_millis(*ms),
        }
    }
}

impl Argument for SetArguments {
    fn parse(mut args: IntoIter<Resp>) -> Result<SetArguments, String> {
        let key = match args.next() {
            Some(Resp::BulkString(b)) => b,
            _ => return Err("ERR key must be a bulk string".to_string()),
        };

        let value = match args.next() { 
          Some(Resp::BulkString(data)) => Record::from_vec(data),
          _ => return Err("ERR key must be a bulk string".to_string())
        };

        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut expiration = None;

        while let Some(arg) = args.next() {
            match arg {
                Resp::BulkString(bs) => {
                    let as_str = String::from_utf8(bs)
                        .map_err(|_| "ERR argument not utf8")?;

                    match &as_str.to_uppercase()[..] {
                        "NX" => nx = true,
                        "XX" => xx = true,
                        "GET" => get = true,
                        "EX" => {
                            if let Some(Resp::BulkString(next_arg)) = args.next() {
                                let seconds = String::from_utf8(next_arg)
                                    .map_err(|_| "ERR invalid seconds format")?
                                    .parse::<u64>()
                                    .map_err(|_| "ERR invalid seconds value")?;
                                expiration = Some(Expiration::Seconds(seconds));
                            } else {
                                return Err("ERR expected seconds after 'EX'".to_string());
                            }
                        },
                        "PX" => { 
                            if let Some(Resp::BulkString(next_arg)) = args.next() {
                                let milliseconds = String::from_utf8(next_arg)
                                    .map_err(|_| "ERR invalid milliseconds format")?
                                    .parse::<u64>()
                                    .map_err(|_| "ERR invalid milliseconds value")?;
                                expiration = Some(Expiration::Milliseconds(milliseconds));
                            } else {
                                return Err("ERR expected milliseconds after 'PX'".to_string());
                            }
                        },
                        _ => return Err("ERR unknown or unexpected argument".to_string()),
                    }
                },
                _ => return Err("ERR arguments must be bulk strings".to_string()),
            }
        }

        Ok(SetArguments { key, value, nx, xx, get, expiration })
    }
}

pub struct ArgumentParser;

impl ArgumentParser {
    pub fn get_from(mut args: IntoIter<Resp>) -> Result<CommandArgument, String> 
    {
        match args.next() {
            Some(resp) => {
                let name: String = resp
                    .try_into()
                    .map_err(|_| "ERR unknown or unexpected command".to_string())?;

                match name.to_uppercase().as_str() {
                    "PING" => Ok(CommandArgument::Ping),
                    "INFO" => Ok(CommandArgument::Info),
                    "GET" => Ok(CommandArgument::Get(GetArguments::parse(args)?)),
                    "SET" => Ok(CommandArgument::Set(SetArguments::parse(args)?)),
                    "ECHO" => Ok(CommandArgument::Echo(EchoArguments::parse(args)?)),
                    "REPLCONF" => Ok(CommandArgument::Replconf(ReplconfArguments::parse(args)?)),
                    "PSYNC" => Ok(CommandArgument::Psync(PsyncArguments::parse(args)?)),
                    _ => Err("ERR unknown or unexpected command".to_string())
                }
            }
            _ => Err("Empty arguments".to_string())
        }
    
    }
}