use crate::resp::Resp;
use crate::database::Record;
use std::time::Duration;

// arguments for the echo command...
#[derive(Debug, Clone)]
pub struct EchoArguments {
    pub message: Resp,
}

impl EchoArguments {
    pub fn parse(mut args: Vec<Resp>) -> Result<EchoArguments, String> {
        if args.len() != 1 {
            return Err("ERR wrong number of arguments for 'echo' command".to_string());
        }

        // Since we check that there is exactly one argument, we can safely pop it
        let message = args.pop().unwrap();

        // Ensure the message is a bulk string; otherwise, return an error
        if let Resp::BulkString(_) = message {
            Ok(EchoArguments { message })
        } else {
            Err("ERR argument must be a bulk string".to_string())
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetArguments {
    pub key: Vec<u8>,
}

impl GetArguments {
    pub fn parse(mut args: Vec<Resp>) -> Result<GetArguments, String> {
        if args.len() != 1 {
            return Err("ERR wrong number of arguments for 'get' command".to_string());
        }

        // Since we check that there is exactly one argument, we can safely pop it
        let key = match args.pop().unwrap() {
            Resp::BulkString(b) => b,
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

impl SetArguments {
    pub fn parse(mut args: Vec<Resp>) -> Result<SetArguments, String> {
        if args.len() < 2 {
            return Err("ERR wrong number of arguments for 'set' command".to_string());
        }
 
        let key = match args.pop().unwrap() {
            Resp::BulkString(b) => b,
            _ => return Err("ERR key must be a bulk string".to_string()),
        };

        let value = Record::from_resp(args.pop().unwrap())
            .ok_or("ERR value must be a bulk string")?;

        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut expiration = None;


        while let Some(arg) = args.pop() {
            match arg {
                Resp::BulkString(bs) => {
                    match bs.as_slice() {
                        b"NX" => nx = true,
                        b"XX" => xx = true,
                        b"GET" => get = true,
                        b"EX" => {
                            if let Some(Resp::BulkString(next_arg)) = args.pop() {
                                let seconds = String::from_utf8(next_arg.clone())
                                    .map_err(|_| "ERR invalid seconds format")?
                                    .parse::<u64>()
                                    .map_err(|_| "ERR invalid seconds value")?;
                                expiration = Some(Expiration::Seconds(seconds));
                            } else {
                                return Err("ERR expected seconds after 'EX'".to_string());
                            }
                        },
                        b"PX" => { 
                            if let Some(Resp::BulkString(next_arg)) = args.pop() {
                                let milliseconds = String::from_utf8(next_arg.clone())
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
