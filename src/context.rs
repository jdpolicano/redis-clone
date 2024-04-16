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
    pub async fn handle_all(self) -> io::Result<()> {
        if self.info.is_replica() {
            self.replica_exec_all().await
        } else {
            self.master_exec_all().await
        }
    }
    
    // only handle a limited command set for this client. 
    // this is used so the replica can receive and respond to certain commands without actually executing them.
    // i.e., you can get info on the replica, but only the connection to the master will allow write commands.
    pub async fn handle_limited(mut self) -> io::Result<()> {
        loop {
            let (message, _msg_len) = self.stream.read_message().await?;
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
    
                Cmd::Get(c) => {
                    let handle = Handle {
                        database: self.database.clone(),
                        history: self.history.clone(),
                        info: self.info.clone()
                    };
    
                    c.execute(
                        &mut self.stream, 
                        handle
                    ).await;
                },
    
                _ => {
                    self.stream.write_err("ERR direct messaging to replica not allowed").await?;
                    // should error here but ok for now.
                    return Ok(());
                }
            }
        }
    }

    async fn master_exec_all(mut self) -> io::Result<()> {
        loop {
            let (message, _) = self.stream.read_message().await?;
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
                    
                    let transaction = valid_cmd.execute(
                        &mut self.stream, 
                        handle
                    ).await;

                    match transaction {
                        Transaction::Replicate => {
                            // preserver this connection and move on.
                            self.history.add_replica(self.stream).await;
                            break Ok(());
                        }

                        Transaction::Write => {
                            self.history.add_write(message).await;
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    async fn replica_exec_all(mut self) -> io::Result<()> {
        loop {
            let (message, msg_len) = self.stream.read_message().await?;
            let cmd = CmdParser::parse(message.clone());
      
            match cmd {
                Cmd::Unexpected(err_msg) => {
                    self.stream.write_err(&format!("ERR {}", err_msg)).await?;
                }
    
                Cmd::ReplConf(c) => {
                    let handle = Handle {
                        database: self.database.clone(),
                        history: self.history.clone(),
                        info: self.info.clone()
                    };
    
                    c.execute(
                        &mut self.stream, 
                        handle
                    ).await;
                }
    
                valid_cmd => {
                    let handle = Handle {
                        database: self.database.clone(),
                        history: self.history.clone(),
                        info: self.info.clone()
                    };

                    self.stream.close_write(); // close the write end of the stream. no need to send back messages right now.
                    
                    let _ = valid_cmd.execute(
                        &mut self.stream, 
                        handle
                    ).await;

                    self.stream.open_write(); // open the write end of the stream again.
                }
            }

            // after the command is processed update the offset we're at.
            // in the error case need to decide if this should be incremented or not?
            self.info.incr_master_repl_offset(msg_len as i64);
        }
    }
}