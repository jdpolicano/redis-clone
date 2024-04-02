use crate::resp::{ Resp, RespParser, RespEncoder };
use bytes::{ BytesMut };
use tokio::net::{ TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt, BufStream };
use std::io::{ self };

#[derive(Debug)]
pub struct Connection {
    // the stream to read an write to.
    stream: TcpStream,
    // a read buffer for incoming data.
    read_buf: BytesMut,
    // a write buffer for incoming data.
    write_buf: BytesMut,
    // these can be used later to modify the behavior of connections and make, for instance,
    // replicas replies always be no-ops rather than actual data while maintaining the same exact 
    // implementation as the master server. 
    writable: bool,
    readable: bool
}

pub enum Error {
    ParseError(&'static str),
    ConnectionClosed,
    IoError(io::Error),
    NotReadable,
    NotWritable,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IoError(e)
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self  {
        match e {
            Error::ParseError(e) => { return io::Error::new(io::ErrorKind::InvalidInput, e) },
            Error::ConnectionClosed => { return io::Error::new(io::ErrorKind::ConnectionAborted, "Connection Closed") },
            Error::IoError(e) => { return e }
            _ => { return io::Error::new(io::ErrorKind::Other, "Noop") }
        }
    }
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: stream,
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(4 * 1024),
            writable: true,
            readable: true,
        }
    }

    pub async fn read_message(&mut self) -> Result<Resp, Error> {
        if !self.readable { return Err(Error::NotReadable) }

        loop {
            // is it guaranteed we didn't write into the buffer at this point?
            let nbytes = self.stream.read_buf(&mut self.read_buf).await?;

            if nbytes == 0 {
                self.read_buf.clear();
                return Err(Error::ConnectionClosed)
            }

            let mut parser = RespParser::new(&self.read_buf);

            // to-do resp parser should tell us if its invalid or eof issue...
            if let Ok(resp) = parser.parse() {
                self.read_buf.clear();
                return Ok(resp)
            }
        }
    }

    // takes a resp encoded value and writes it to the buffer...
    pub async fn write_message(&mut self, payload: &Resp) -> Result<(), Error> {
        if !self.writable { return Err(Error::NotWritable) }
        RespEncoder::encode_resp(payload, &mut self.write_buf);
        let result = self.stream.write_all(&self.write_buf).await;
        self.write_buf.clear();
        Ok(result?)
    }

    pub async fn write_str(&mut self, payload: &str) -> Result<(), Error> {
        if !self.writable { return Err(Error::NotWritable) }
        RespEncoder::encode_simple_string(payload, &mut self.write_buf);
        let result = self.write_bytes(payload.as_bytes()).await;
        self.write_buf.clear();
        Ok(result?)
    }

    pub async fn write_err(&mut self, payload: &str) -> Result<(), Error> {
        if !self.writable { return Err(Error::NotWritable) }
        RespEncoder::encode_simple_error(payload, &mut self.write_buf);
        let result = self.write_bytes(payload.as_bytes()).await;
        self.write_buf.clear();
        Ok(result?)
    }

    pub async fn write_bytes(&mut self, payload: &[u8]) -> Result<(), Error> {
        if !self.writable { return Err(Error::NotWritable) }
        RespEncoder::encode_bulk_string(payload, &mut self.write_buf);
        let result = self.stream.write_all(&self.write_buf).await;
        self.write_buf.clear();
        Ok(result?)
    }

    pub fn write(&mut self, payload: &[u8]) {
        if self.writable { return; }
        self.write_buf.extend_from_slice(payload);
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub fn close_write(&mut self) {
        self.writable = false;
    }

    pub fn close_read(&mut self) {
        self.readable = false;
    }

    pub fn open_write(&mut self) {
        self.writable = true;
    }

    pub fn open_read(&mut self) {
        self.readable = true;
    }

    pub fn take_stream(self) -> TcpStream {
        self.stream
    }

    pub fn borrow_stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
}