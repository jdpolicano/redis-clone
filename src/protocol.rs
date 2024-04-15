// use crate::command::Command; // the command trait for a given handler.
use crate::client::RedisClient; // the client struct for a given connection.
use crate::context::Handle;
use crate::resp::Resp;
use crate::connection::Connection;
use crate::internals::{ ReplconfArguments };
use crate::arguments::{ CommandArgument, ArgumentParser };
use std::io;
// the state transitions for redis instance communications via replconf + psync,
// for now this is only used in a state machine that will drive these negoationations
// on both the client and server side, but eventually there may be more.

// the current capabilities of the protocol state machine are to intiate and start a replication session
// and to handle ack from the server which will tell if a replica is still living.
#[derive(Debug, PartialEq)]
pub enum ReplClientState {
    // starting position
    Initial,
    // ping the server to see if it is alive
    Ping,
    // client has sent the listening port to the server
    NotifyListeningPort,
    // client has sent the capabilities to the server
    NotifyCapabilities,
    // client has requested a psync from the server
    RequestPsync,
    // client has received the full resync from the server
    FullResync,
    // client has received the rdb file from the server and is replicating
    ReplicationStart,
    // client has received an ack from the server
    ProtocolError(&'static str),
}

#[derive(Debug, PartialEq)]
pub enum ReplicationEvents {
    Start,
    RecievedPong,
    ReceivedOk,
    ReceivedFullResync,
    ReceivedRDB,
    ReceivedAck,
    ProtocolError(&'static str),
}

impl ReplClientState {
    pub fn handle_event(&self, event: ReplicationEvents) -> ReplClientState {
        match (self, event) {
            (ReplClientState::Initial, ReplicationEvents::Start) => ReplClientState::Ping,
            (ReplClientState::Ping, ReplicationEvents::RecievedPong) => ReplClientState::NotifyListeningPort,
            (ReplClientState::NotifyListeningPort, ReplicationEvents::ReceivedOk) => ReplClientState::NotifyCapabilities,
            (ReplClientState::NotifyCapabilities, ReplicationEvents::ReceivedOk) => ReplClientState::RequestPsync,
            (ReplClientState::RequestPsync, ReplicationEvents::ReceivedFullResync) => ReplClientState::FullResync,
            (ReplClientState::FullResync, ReplicationEvents::ReceivedRDB) => ReplClientState::ReplicationStart,
            (ReplClientState::ReplicationStart, ReplicationEvents::ReceivedAck) => ReplClientState::ReplicationStart,
            (_, ReplicationEvents::ProtocolError(err_msg)) => ReplClientState::ProtocolError(err_msg),
            _ => ReplClientState::ProtocolError("Invalid State Transition")
        }
    }
}

// the proctocol wrapper for the client to intiate a replication session
pub struct ReplicationProtocol<'a> {
    pub state: ReplClientState,
    pub listening_port: String,
    pub handle: Handle,
    pub client: RedisClient<'a>,
}

impl<'a> ReplicationProtocol<'a> {
    pub fn new(client: RedisClient<'a>, listening_port: String, handle: Handle) -> Self {
        ReplicationProtocol {
            state: ReplClientState::Initial,
            listening_port,
            handle,
            client,
        }
    }

    pub fn handle_event(&mut self, event: ReplicationEvents) {
        self.state = self.state.handle_event(event);
    }

    pub async fn start(&mut self) -> io::Result<()> {
        loop {
            match self.state {
                ReplClientState::Initial => { self.handle_event(ReplicationEvents::Start); },

                ReplClientState::Ping => {
                    self.client.ping().await?;
                    let next_event = self.handle_response().await;
                    self.handle_event(next_event);
                },

                ReplClientState::NotifyListeningPort => {
                    self.client.repl_conf(&["listening-port", &self.listening_port]).await?;
                    let next_event = self.handle_response().await;
                    self.handle_event(next_event);
                },

                ReplClientState::NotifyCapabilities => {
                    self.client.repl_conf(&["capa", "psync2"]).await?;
                    let next_event = self.handle_response().await;
                    self.handle_event(next_event);
                },

                ReplClientState::RequestPsync => {
                    let master_replid = self.handle.info.get_master_replid();
                    let master_repl_offset = self.handle.info.get_master_repl_offset().to_string();
                    self.client.psync(&[&master_replid, &master_repl_offset]).await?;
                    let next_event = self.handle_response().await;
                    self.handle_event(next_event);
                },

                ReplClientState::FullResync => {
                    let next_event = self.handle_response().await;
                    self.handle_event(next_event);
                },

                ReplClientState::ReplicationStart => {
                    return Ok(())
                },

                _ => {
                    println!("ERR {:?}", self.state);
                    return Ok(())
                }
            }
        }
    }

    // used to route to a response handler based on the current state and return
    // a new event.
    pub async fn handle_response(&mut self) -> ReplicationEvents {
        match self.state {
            ReplClientState::Ping => { self.expect_pong().await },
            ReplClientState::NotifyListeningPort => { self.expect_ok().await },
            ReplClientState::NotifyCapabilities => { self.expect_ok().await },
            ReplClientState::RequestPsync => { self.expect_full_resync().await },
            ReplClientState::FullResync => { self.expect_rdb().await },
            _ => ReplicationEvents::ProtocolError("Invalid state, no response handler found.")
        }
    }
    
    pub async fn expect_pong(&mut self) -> ReplicationEvents {
        let resp = self.client.read_message().await;
        match resp {
            Ok(Resp::SimpleString(s)) if s == "PONG" => ReplicationEvents::RecievedPong,
            _ => ReplicationEvents::ProtocolError("Invalid response from server, expected PONG in simple string format.")
        }
    }

    pub async fn expect_ok(&mut self) -> ReplicationEvents {
        let resp = self.client.read_message().await;
        match resp {
            Ok(Resp::SimpleString(s)) if s == "OK" => ReplicationEvents::ReceivedOk,
            _ => ReplicationEvents::ProtocolError("Invalid response from server, expected OK in simple string format.")
        }
    }

    // should expect  a bulk string in the format of
    // "FULLRESYNC <replid> <offset>". It should split by space
    // and update the server infos replid and offset (offset should be parsed into a i64)
    pub async fn expect_full_resync(&mut self) -> ReplicationEvents {
        let resp = self.client.read_message().await;
        match resp {
            Ok(Resp::BulkString(s)) => {
                let parsed_str = std::str::from_utf8(&s);

                if parsed_str.is_err() {
                    return ReplicationEvents::ProtocolError("ERR full resync string is not valid utf8");
                }

                let parsed_str = parsed_str.unwrap();

                let parts: Vec<&str> = parsed_str
                    .split(" ")
                    .collect();

                if parts.len() == 3 && parts[0] == "FULLRESYNC" {
                    self.handle.info.set_master_replid(parts[1].to_string());
                    self.handle.info.set_master_repl_offset(parts[2].parse::<i64>().unwrap());
                    ReplicationEvents::ReceivedFullResync
                } else {
                    ReplicationEvents::ProtocolError("ERR full resync should be 'FULLRESYNC <replid> <offset>'")
                }
            },
            _ => ReplicationEvents::ProtocolError("Invalid response from server, expected bulk string.")
        }
    }


    pub async fn expect_rdb(&mut self) -> ReplicationEvents {
        let resp = self.client.read_rdb().await;
        match resp {
            // for now this is a no-op, but eventually we will want to write this to the database.
            Ok(_) => ReplicationEvents::ReceivedRDB,
            _ => ReplicationEvents::ProtocolError("Invalid response from server, expected rdb file.")
        }
    }
}


#[derive(Debug, PartialEq)]
pub enum ReplServerState {
    // server has received a request to replicate data, this is the start of a replication session
    RecievedListeningPort,
    // server has received the capabilities from the client
    RecievedCapabilities,
    // server has received a request to start psync
    RecievedPsync,
    // server has sent a full resync to the client
    SentFullResync,
    // server has sent an rdb file to the client
    SentRDB,
    // This client is now a replica, break.
    ReplicationComplete,
    // something f*cked up.
    ProtocolError(&'static str)
}

#[derive(Debug, PartialEq)]
pub enum ReplServerEvents {
    SentResponse,
    NotifyCapabilities,
    NotifyPsync,
    Done,
    ProtocolError(&'static str)
}

impl ReplServerState {
    pub fn handle_event(&self, event: ReplServerEvents) -> ReplServerState {
        match (self, event) {
            (ReplServerState::RecievedListeningPort, ReplServerEvents::NotifyCapabilities) => ReplServerState::RecievedCapabilities,
            (ReplServerState::RecievedCapabilities, ReplServerEvents::NotifyPsync) => ReplServerState::RecievedPsync,
            (ReplServerState::RecievedPsync, ReplServerEvents::SentResponse) => ReplServerState::SentFullResync,
            (ReplServerState::SentFullResync, ReplServerEvents::SentResponse) => ReplServerState::SentRDB,
            (ReplServerState::SentRDB, ReplServerEvents::Done) => ReplServerState::ReplicationComplete,
            (_, ReplServerEvents::ProtocolError(s)) => ReplServerState::ProtocolError(s),
            _ => ReplServerState::ProtocolError("Invalid State Transition")
        }
    }
}

pub struct ReplServerProtocol<'a> {
    pub state: ReplServerState,
    // notice it owns the connection because after this transaction the connection will
    // be moved elsewhere to be followed on with.
    pub stream: &'a mut Connection,
    pub handle: Handle,
}


impl<'a> ReplServerProtocol<'a> {
    pub fn new(stream: &'a mut Connection, handle: Handle) -> Self {
        ReplServerProtocol {
            state: ReplServerState::RecievedListeningPort,
            stream,
            handle,
        }
    }

    pub fn handle_event(&mut self, event: ReplServerEvents) {
        self.state = self.state.handle_event(event);
    }

    pub async fn start(&mut self) -> io::Result<()> {
        loop {
            match self.state {
                ReplServerState::RecievedListeningPort => {
                    self.stream.write_str("OK").await?;
                    let next_event = self.handle_response().await?;
                    self.handle_event(next_event);
                },

                ReplServerState::RecievedCapabilities => {
                    self.stream.write_str("OK").await?;
                    let next_event = self.handle_response().await?;
                    self.handle_event(next_event);
                },

                ReplServerState::RecievedPsync => {
                    let replid = self.handle.info.get_master_replid();
                    let offset = self.handle.info.get_master_repl_offset().to_string();
                    let payload = format!("FULLRESYNC {} {}", replid, offset);
                    self.stream.write_bytes(&payload.as_bytes()).await?;
                    self.handle_event(ReplServerEvents::SentResponse);  
                },


                ReplServerState::SentFullResync => {
                    let rdb_file = get_empty_rdb_file();
                    self.stream.write(format!("${}\r\n", rdb_file.len()).as_bytes());
                    self.stream.write(&rdb_file);
                    let _ = self.stream.flush().await;
                    self.handle_event(ReplServerEvents::SentResponse);
                },

                ReplServerState::SentRDB => {
                    // perform some kind of check maybe enventually and make sure stuff
                    // happened correctly.
                    self.handle_event(ReplServerEvents::Done);
                    return Ok(())
                },

                ReplServerState::ReplicationComplete => {
                    return Ok(())
                },

                _ => {
                    println!("Protocol Error {:?}", self.state);
                    panic!("negotiation failed");
                }
            }
        }
    }

    async fn handle_response(&mut self) -> io::Result<ReplServerEvents> {
        match self.state {
            ReplServerState::RecievedListeningPort => { self.expect_capabilities().await },
            ReplServerState::RecievedCapabilities => { self.expect_psync().await },
            _ => Err(io::Error::new(io::ErrorKind::Other, "Protocol Error"))
        }
    }

    async fn expect_capabilities(&mut self) -> io::Result<ReplServerEvents> {
        let message = self.stream.read_message().await?;

        if let Some(args) = message.as_vec() {
            let parsed_args = self.get_command_arg(args)?;

            if let CommandArgument::Replconf(ReplconfArguments::Capa(capa)) = parsed_args {
                if capa == "psync2" {
                    return Ok(ReplServerEvents::NotifyCapabilities)
                }
            }
            return Ok(ReplServerEvents::ProtocolError("ERR expected capa psync2"))
        }

        Ok(ReplServerEvents::ProtocolError("ERR expected array for capabilities"))
    }

    async fn expect_psync(&mut self) -> io::Result<ReplServerEvents> {
        let message = self.stream.read_message().await?;

        if let Some(arr) = message.as_vec() {
            let parsed_args = self.get_command_arg(arr)?;

            if let CommandArgument::Psync(_psync_args) = parsed_args {
                // to-do handle the psync replid and reploffset...
                return Ok(ReplServerEvents::NotifyPsync)
            }
        }

        Ok(ReplServerEvents::ProtocolError("ERR expected full resync"))

    }

    fn get_command_arg(&self, args: Vec<Resp>) -> io::Result<CommandArgument> {
        ArgumentParser::get_from(args.into_iter())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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