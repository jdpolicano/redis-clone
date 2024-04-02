use tokio::net::{ TcpStream };
use tokio::io::{ AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use crate::resp::{ Resp, RespEncoder };
use crate::connection::Connection;
// use crate::server::{ read_and_parse};

pub struct RedisClient<'a> {
    pub stream: &'a mut Connection
}

impl<'a> RedisClient<'a> {
    // create a client from an existing stream
    pub fn from_stream(stream: &'a mut Connection) -> Self {
        RedisClient {
            stream
        }
    }

    pub async fn ping(&mut self) -> io::Result<()> {
        let mut arguments = Vec::new();

        self.add_arg("PING", &mut arguments);
        let resp_arr = Resp::Array(arguments);

        self.stream.write_message(&resp_arr).await?;
        let response = self.stream.read_message().await?;
        self.assert_res_is_simple(response, "PONG")
    }

    pub async fn repl_conf(&mut self, args: &[&str]) -> io::Result<()> {
        let mut arguments = Vec::new();

        self.add_arg("REPLCONF", &mut arguments);
        for arg in args {
            self.add_arg(arg, &mut arguments);
        }
        
        let resp_arr = Resp::Array(arguments);
        self.stream.write_message(&resp_arr).await?;
        let response = self.stream.read_message().await?;
        self.assert_res_ok(response)
    }

    pub async fn psync(&mut self, args: &[&str]) -> io::Result<()> {
        let mut arguments = Vec::new();

        self.add_arg("PSYNC", &mut arguments);
        for arg in args {
            self.add_arg(arg, &mut arguments);
        }
        
        let resp_arr = Resp::Array(arguments);
        self.stream.write_message(&resp_arr).await?;
        let response = self.stream.read_message().await?;
        println!("{:?}", response);
        Ok(())
    }

    fn add_arg(&self, arg: &str, container: &mut Vec<Resp>) {
        container.push(Resp::BulkString(arg.as_bytes().to_vec()));
    }
    
    fn assert_res_ok(&self, actual: Resp) -> io::Result<()> {
        self.assert_res_is_simple(actual, "OK")
    }

    fn assert_res_is_simple(&self, actual: Resp, expected: &str) -> io::Result<()> {
        let err = Err(io::Error::new(io::ErrorKind::InvalidInput, "Expected OK response"));

        match actual {
            Resp::SimpleString(s) => {
                if s != expected {
                    return err
                }
            },
            _ => {
                return err
            }
        }

        Ok(())
    }
}