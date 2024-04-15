use std::io;
use std::sync::Arc;
use crate::connection::Connection;
use crate::database::Database; 
use crate::history::History;
use crate::server::ServerInfo;
use crate::command::{CmdParser, Cmd, Command, Transaction};

// this is a handler that can be passed around to simplify function signatures etc...
pub struct Handle {
    pub database: Arc<Database>,
    pub history: Arc<History>,
    pub info: Arc<ServerInfo>
}

// The state of the request response cycle for each client request...
pub struct Context {
    pub stream: Connection, // the currently connected client.
    pub database: Arc<Database>, // database to alter if need be.
    pub history: Arc<History>, // struct for writing to replicas and recording transactions.
    pub info: Arc<ServerInfo> // information about the current server running.
}

impl Context {
    pub fn new(stream: Connection, database: Arc<Database>, history: Arc<History>, info: Arc<ServerInfo>) -> Self {
        Context {
            stream,
            database,
            history,
            info
        }
    }

    // handle all commands with unlimited functionality.
    pub async fn handle_all(mut self) -> io::Result<()> {
        loop {
            let message = self.stream.read_message().await?;
            let cmd = CmdParser::parse(message.clone());
    
            match cmd {
                Cmd::Unexpected(err_msg) => {
                    self.stream.write_err(&format!("ERR {}", err_msg)).await?;
                }
    
                valid_cmd => {
                    let handle = Handle {
                        database: self.database.clone(),
                        history: self.history.clone(),
                        info: self.info.clone()
                    };

                    if self.info.is_replica() {
                        self.stream.close_write(); // close the write end of the stream. no need to send back messages right now.
                    }
                    
                    let transaction = valid_cmd.execute(
                        &mut self.stream, 
                        handle
                    ).await;

                    match transaction {
                        Transaction::Replicate if !self.info.is_replica() => {
                            // preserver this connection and move on.
                            self.history.add_replica(self.stream).await;
                            break Ok(());
                        }

                        Transaction::Write if !self.info.is_replica() => {
                            self.history.add_write(message).await;
                        }

                        _ => {

                        }
                    }
                }
            }
        }
    }
    
    // only handle a limited command set for this client. 
    // this is used so the replica can receive and respond to certain commands without actually executing them.
    // i.e., you can get info on the replica, but only the connection to the master will allow write commands.
    pub async fn handle_limited(mut self) -> io::Result<()> {
        let message = self.stream.read_message().await?;
        let cmd = CmdParser::parse(message.clone());
        match cmd {
            Cmd::Info(c) => {
                let handle = Handle {
                    database: self.database.clone(),
                    history: self.history.clone(),
                    info: self.info.clone()
                };

                c.execute(
                    &mut self.stream, 
                    handle
                ).await;

                return Ok(());
            },

            _ => {
                self.stream.write_err("ERR direct messaging to replica not allowed").await?;
                return Ok(());
            }
        }
    }
}