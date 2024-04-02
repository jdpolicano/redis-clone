use std::io;
use std::sync::Arc;
use crate::connection::Connection;
use crate::database::Database; 
use crate::history::History;
use crate::server::ServerInfo;
use crate::command::{CmdParser, Cmd, Command, Transaction};

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

    pub async fn handle(mut self) -> io::Result<()> {
        loop {
            let message = self.stream.read_message().await?;
            let cmd = CmdParser::parse(message.clone());
            
            if self.info.is_replica() {
                println!("received replica command: {:?}", message);
            }
    
            match cmd {
                Cmd::Unknown => {
                    self.stream.write_err("ERR unknown command name").await?;
                }
    
                Cmd::Unexpected(err_msg) => {
                    self.stream.write_err(&format!("ERR {}", err_msg)).await?;
                }
    
                valid_cmd => {
                    let transaction = valid_cmd.execute(&mut self.stream, self.database.clone(), self.info.clone(), self.history.clone()).await;

                    match transaction {
                        Transaction::Replicate if !self.info.is_replica() => {
                            // preserver this connection and move on.
                            self.history.add_replica(self.stream).await;
                            break;
                        }

                        Transaction::Write if !self.info.is_replica() => {
                            println!("writing to replicas from master...");
                            self.history.add_write(message).await;
                        }

                        _ => {

                        }
                    }
                }
            }
        }

        Ok(())
    }
}