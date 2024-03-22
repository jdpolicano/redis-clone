use tokio::net::{ TcpStream };
use tokio::io::{ AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use crate::resp::{ Resp, RespParser, RespEncoder};
use crate::server::{ read_and_parse};

// a struct containing the reponse from the server...
pub struct RedisResponse {
    raw: BytesMut,
}

impl RedisResponse {
    pub fn new(raw: BytesMut) -> Self {
        RedisResponse {
            raw,
        }
    }

    pub fn as_resp(&self) -> Result<Resp, String> {
        let mut parser = RespParser::new(self.raw.clone());
        parser.parse()
    }
}

pub struct RequestBuilder {
    args: Vec<Resp>
}

impl RequestBuilder {
    pub fn new() -> Self {
        RequestBuilder {
            args: Vec::new(),
        }
    }

    pub fn arg(&mut self, arg: &str) -> &mut Self {
        self.add_arg(arg.as_bytes())
    }

    fn add_arg(&mut self, arg: &[u8]) -> &mut Self {
        self.args.push(Resp::BulkString(arg.to_vec()));
        self
    }
    pub fn build(&mut self) -> BytesMut {
        let mut bytes = BytesMut::new();
        RespEncoder::encode_array(&self.args, &mut bytes);
        bytes
    }
}

pub struct RedisClient<'a> {
    pub stream: &'a mut TcpStream
}

impl<'a> RedisClient<'a> {
    // create a client from an existing stream
    pub fn from_stream(stream: &'a mut TcpStream) -> Self {
        RedisClient {
            stream
        }
    }

    pub async fn ping(&mut self) -> io::Result<()> {
        let write_buf = RequestBuilder::new()
            .arg("PING")
            .build();

        self.stream.write_all(&write_buf).await?;

        let mut read_buf = BytesMut::new();
        let resp = read_and_parse(self.stream, &mut read_buf).await?;
        self.assert_res_is_simple(resp, "PONG")
    }

    pub async fn repl_conf(&mut self, args: &[&str]) -> io::Result<()> {
        let mut builder = RequestBuilder::new();

        builder.arg("REPLCONF");

        for arg in args {
            builder.arg(arg);
        }

        let write_buf = builder.build();

        self.stream.write_all(&write_buf).await?;

        let mut read_buf = BytesMut::new();
        let resp = read_and_parse(self.stream, &mut read_buf).await?;
        self.assert_res_ok(resp)
    }

    
    pub fn assert_res_ok(&self, actual: Resp) -> io::Result<()> {
        self.assert_res_is_simple(actual, "OK")
    }

    pub fn assert_res_is_simple(&self, actual: Resp, expected: &str) -> io::Result<()> {
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