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

    // returns the resp decoded value and a number indication how large the original
    // message was.
    pub async fn read_message(&mut self) -> Result<(Resp, u64), Error> {
        self.ensure_readable()?;

        while !self.is_complete_message()? {
            self.fill_buffer().await?;
        }

        let start_pos = self.read_buf.position();
        let mut parser = RespParser::new(&mut self.read_buf);
        let res = parser.parse().map_err(Error::ParseError)?;
        Ok((res, self.read_buf.position() - start_pos))
    }

    pub async fn read_rdb(&mut self) -> Result<Vec<u8>, Error> {
        self.ensure_readable()?;

        while !self.is_complete_rdb()? {
            self.fill_buffer().await?;
        }

        self.parse_rdb()
    }

    async fn fill_buffer(&mut self) -> Result<(), Error> {
        let nbytes = self.stream.read_buf(&mut self.read_buf.get_mut()).await?;
        if nbytes == 0 {
            self.read_buf.get_mut().clear();
            self.read_buf.set_position(0);
            return Err(Error::ConnectionClosed);
        }
        Ok(())
    }

    fn ensure_readable(&self) -> Result<(), Error> {
        if !self.readable {
            Err(Error::NotReadable)
        } else {
            Ok(())
        }
    }

    fn is_complete_message(&mut self) -> Result<bool, Error> {
        let mut parser = RespParser::new(&mut self.read_buf);
        match parser.check() {
            Ok(_) => Ok(true),
            Err(ParseError::UnexpectedEndOfInput) => Ok(false),
            Err(e) => Err(Error::ParseError(e)),
        }
    }

    fn is_complete_rdb(&mut self) -> Result<bool, Error> {
        match self.try_parse_rdb() {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    // TEMPORARY UNTIL WE ADD BONEFIDE RDB PARSING
    fn parse_rdb(&mut self) -> Result<Vec<u8>, Error> {
        let first = next_byte(&mut self.read_buf)?;
        let len_bytes = parse_until_crlf(&mut self.read_buf)?;
        let len_str = std::str::from_utf8(&len_bytes).map_err(|_| Error::ParseError(ParseError::InvalidByte))?;
        let len = len_str.parse::<usize>().map_err(|_| Error::ParseError(ParseError::InvalidByte))?;
        let data = parse_n_bytes(&mut self.read_buf, len)?;
        Ok(data)
    }

    // TEMPORARY UNTIL WE ADD BONEFIDE RDB PARSING
    fn try_parse_rdb(&mut self) -> Result<(), Error> {
        let start_pos = self.read_buf.position(); // get the current position.
        let _ = self.parse_rdb()?;
        self.read_buf.set_position(start_pos); // set it back
        Ok(())
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