// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener };
use std::sync::Mutex;
use std::io;
use std::env;
use crate::database::{ Database };
use crate::listener::{ Listener };
use crate::history::History;

#[derive(Debug)]
pub struct ServerInfo {
    inner: Mutex<ServerInfoInner>
}

impl ServerInfo {
    pub fn new(master_host: Option<(String, String)>) -> Self {
        let info = ServerInfoInner::new(master_host);
        Self {
            inner: Mutex::new(info)
        }
    }

    pub fn master() -> Self {
        Self::new(None)
    }

    pub fn replica(master_host: (String, String)) -> Self {
        Self::new(Some(master_host))
    }

    pub fn get_role(&self) -> String {
        self.inner.lock().unwrap().role.clone()
    }

    pub fn get_master_replid(&self) -> String {
        self.inner.lock().unwrap().master_replid.clone()
    }

    pub fn get_master_repl_offset(&self) -> i64 {
        self.inner.lock().unwrap().master_repl_offset.clone()
    }

    pub fn get_master_host(&self) -> Option<String> {
        self.inner.lock().unwrap().master_host.clone()
    }

    pub fn is_replica(&self) -> bool {
        if self.get_role() == "master" { false } else { true }
    }

    pub fn set_master_replid(&self, replid: String) {
        self.inner.lock().unwrap().set_master_replid(replid);
    }

    pub fn set_master_repl_offset(&self, offset: i64) {
        self.inner.lock().unwrap().set_master_repl_offset(offset);
    }
}

#[derive(Debug)]
pub struct ServerInfoInner {
    role: String,
    master_replid: String,
    master_repl_offset: i64,
    master_host: Option<String>,
}

impl ServerInfoInner {
    pub fn new(master_host: Option<(String, String)>) -> Self {
        if master_host.is_some() {
            Self::replica(master_host.unwrap())
        } else {
            Self::master()
        }
    }

    pub fn master() -> Self {
        Self {
            role: "master".to_string(),
            // this will be generated eventually...
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            master_host: None
        }
    }

    pub fn replica(master_host: (String, String)) -> Self {
        Self {
            role: "slave".to_string(),
            master_replid: "?".to_string(),
            master_repl_offset: -1,
            master_host: Some(format!("{}:{}", master_host.0, master_host.1))
        }
    }

    pub fn get_role(&self) -> String {
        self.role.clone()
    }

    pub fn get_master_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub fn get_master_repl_offset(&self) -> i64 {
        self.master_repl_offset.clone()
    }

    pub fn get_master_host(&self) -> Option<String> {
        self.master_host.clone()
    }

    pub fn is_replica(&self) -> bool {
        if self.role == "master" { false } else { true }
    }

    pub fn set_master_replid(&mut self, replid: String) {
        self.master_replid = replid;
    }

    pub fn set_master_repl_offset(&mut self, offset: i64) {
        self.master_repl_offset = offset;
    }
}

pub struct RedisServer {
    pub listener: Listener,
}

impl RedisServer {
    pub async fn bind(args: ServerArguments) -> io::Result<Self> {
        let addr = format!("{}:{}", args.host, args.port);
        let database = Database::new(); 
        let history = History::new();
        let info = ServerInfo::new(args.replica_of);
        
        let tcp_socket = TcpListener::bind(addr.clone()).await?;
        println!("Listening on: {}", addr);

        let listener = Listener::new(tcp_socket, database, history, info);
        Ok(RedisServer { listener })
    }
}

// These arguments do not require a name and do not conform to the general argument parser trait...
pub struct ServerArguments {
    pub host: String,
    pub port: String,
    pub replica_of: Option<(String, String)>,
  }
  
  impl ServerArguments {
      pub fn parse() -> ServerArguments {
          let mut env = env::args();
          let mut port = "6379".to_string();
          let mut replica_of = None;
  
          env.next(); // skip executable path...
  
          while let Some(arg) = env.next() {
              match arg.as_str() {
                  "--port" => {
                      if let Some(n) = env.next() {
                          port = n;
                      } else {
                          println!("no port passed, defaulting to {}", port);
                      }
                  },
  
                  "--replicaof" => {
                      if let Some(repl_host) = env.next() {
                          if let Some(repl_port) = env.next() {
                              replica_of = Some((repl_host, repl_port));
                          }
                      };
                  }
                  _ => println!("recevied unsupported arg {}", arg)
              }
          }
          
          // default to local host for now.
          Self { host: "127.0.0.1".to_string(), port, replica_of }
      }
  
      pub fn is_replica(&self) -> bool {
          self.replica_of.is_some()
      }
  }
    