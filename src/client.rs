use std::io;
use crate::resp::{ Resp };
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
        Ok(())
    }

    pub async fn repl_conf(&mut self, args: &[&str]) -> io::Result<()> {
        let mut arguments = Vec::new();
        self.add_arg("REPLCONF", &mut arguments);

        for arg in args {
            self.add_arg(arg, &mut arguments);
        }
        
        let resp_arr = Resp::Array(arguments);
        self.stream.write_message(&resp_arr).await?;
        Ok(())
    }

    pub async fn psync(&mut self, args: &[&str]) -> io::Result<()> {
        let mut arguments = Vec::new();

        self.add_arg("PSYNC", &mut arguments);
        for arg in args {
            self.add_arg(arg, &mut arguments);
        }
        
        let resp_arr = Resp::Array(arguments);
        self.stream.write_message(&resp_arr).await?;
        Ok(())
    }

    pub async fn read_message(&mut self) -> io::Result<Resp> {
        let resp = self.stream.read_message().await?;
        Ok(resp)
    }

    pub async fn read_rdb(&mut self) -> io::Result<Vec<u8>> {
        let rdb = self.stream.read_rdb().await?;
        Ok(rdb)
    }

    fn add_arg(&self, arg: &str, container: &mut Vec<Resp>) {
        container.push(Resp::BulkString(arg.as_bytes().to_vec()));
    }
}