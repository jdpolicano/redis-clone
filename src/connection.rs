use crate::resp::{ Resp, RespParser, RespEncoder, ParseError };
use bytes::{ BytesMut, Buf };
use tokio::net::{ TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use std::io::{ self, Cursor };

#[derive(Debug)]
pub enum Error {
    ParseError(ParseError),
    IncompleteStream,
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

#[derive(Debug)]
pub struct Connection {
    // the stream to read an write to.
    stream: TcpStream,
    // a read buffer for incoming data.
    read_buf: Cursor<BytesMut>,
    // a write buffer for incoming data.
    write_buf: BytesMut,
    // these can be used later to modify the behavior of connections and make, for instance,
    // replicas replies always be no-ops rather than actual data while maintaining the same exact 
    // implementation as the master server. 
    writable: bool,
    readable: bool
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: stream,
            read_buf: Cursor::new(BytesMut::with_capacity(4 * 1024)),
            write_buf: BytesMut::with_capacity(4 * 1024),
            writable: true,
            readable: true,
        }
    }

    pub async fn read_message(&mut self) -> Result<Resp, Error> {
        if !self.readable { return Err(Error::NotReadable) }

        if !self.should_read().await? {
            let mut parser = RespParser::new(&mut self.read_buf);
            return parser
                .parse()
                .map_err(|e| Error::ParseError(e));
        }

        loop {
            // is it guaranteed we didn't write into the buffer at this point?
            let nbytes = self.stream.read_buf(&mut self.read_buf.get_mut()).await?;
            
            if nbytes == 0 {
                self.read_buf.get_mut().clear();
                self.read_buf.set_position(0);
                return Err(Error::ConnectionClosed)
            }
            
            let mut parser = RespParser::new(&mut self.read_buf);

            match parser.check() {
                Ok(_) => {
                    let resp = parser.parse().unwrap(); // we already know this will work...

                    if self.read_buf.remaining() == 0 {
                        self.read_buf.get_mut().clear();
                        self.read_buf.set_position(0);
                    }

                    return Ok(resp)
                },
                Err(e) => {
                    println!("error: {:?}", e);
                    match e {
                        ParseError::UnexpectedEndOfInput => {
                            continue;
                        },
                        
                        e => {
                            self.write_err("ERR RESP Protocol Error").await?;
                            return Err(Error::ParseError(e))
                        }
                    }
                }
            }
        }
    }
    // TEMPORARY UNTIL WE ADD BONEFIDE RDB PARSING
    pub async fn read_rdb(&mut self) -> Result<Vec<u8>, Error> {
        let _ = next_byte(&mut self.read_buf)?;
        let len_bytes = parse_until_crlf(&mut self.read_buf)?;
        let len_str = std::str::from_utf8(&len_bytes).map_err(|_| Error::ParseError(ParseError::InvalidByte))?;
        let len = len_str.parse::<usize>().map_err(|_| Error::ParseError(ParseError::InvalidByte))?;
        let data = parse_n_bytes(&mut self.read_buf, len)?;
        Ok(data)
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
        let result = self.stream.write_all(&self.write_buf).await;
        self.write_buf.clear();
        Ok(result?)
    }

    pub async fn write_err(&mut self, payload: &str) -> Result<(), Error> {
        if !self.writable { return Err(Error::NotWritable) }
        RespEncoder::encode_simple_error(payload, &mut self.write_buf);
        let result = self.stream.write_all(&self.write_buf).await;
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
        if !self.writable { return; }
        self.write_buf.extend_from_slice(payload);
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();
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

    async fn should_read(&mut self) -> Result<bool, Error> {
        // if there is no new data to read, then we should
        // attempt a read operation.
        if !self.read_buf.has_remaining() {
            return Ok(true)
        }

        let mut parser = RespParser::new(&mut self.read_buf);

        match parser.check() {
            // if the stream has a token to read, we shouldn't try reading more for now.
            Ok(_) => Ok(false),
            Err(e) => {
                println!("error: {:?}", e);
                match e {
                    // if the stream is just incomplete, we should try reading more.
                    ParseError::UnexpectedEndOfInput => Ok(true),

                    // if the stream is invalid, we should write an error and return it.
                    _ => {
                        self.write_err("ERR RESP Protocol Error").await?;
                        return Err(Error::ParseError(e))
                    }
                }
            }
        }
    }
}

// Temporary helpers for reading an rdb file...
fn next_byte(buf: &mut Cursor<BytesMut>) -> Result<u8, Error> {
    if buf.has_remaining() {
        Ok(buf.get_u8())
    } else {
        Err(Error::ParseError(ParseError::UnexpectedEndOfInput))
    }
}

fn parse_until_crlf(buf: &mut Cursor<BytesMut>) -> Result<Vec<u8>, Error> {
    let mut result = Vec::new();
    loop {
        let byte = next_byte(buf)?;
        if byte == b'\r' {
            let next_byte = next_byte(buf)?;
            if next_byte == b'\n' {
                return Ok(result)
            } else {
                result.push(byte);
                result.push(next_byte);
            }
        } else {
            result.push(byte);
        }
    }
}

fn parse_n_bytes(buf: &mut Cursor<BytesMut>, n: usize) -> Result<Vec<u8>, Error> {
    if buf.remaining() < n {
        return Err(Error::ParseError(ParseError::UnexpectedEndOfInput))
    }
    let mut result = Vec::with_capacity(n);
    for _ in 0..n {
        result.push(next_byte(buf)?);
    }
    Ok(result)
}