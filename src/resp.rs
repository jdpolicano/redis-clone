use bytes::{ BytesMut, BufMut, Buf };
use std::io::Cursor;
use std::fmt::Display;
use std::error::Error;
// represents all of the possible types within a resp encoded string
// RESP data type	Minimal protocol version	Category	First byte
// Simple strings	RESP2	                    Simple	    +
// Simple Errors	RESP2	                    Simple	    -
// Integers  	    RESP2	                    Simple	    :
// Bulk strings	    RESP2	                    Aggregate	$
// Arrays	        RESP2	                    Aggregate	*
// Nulls	        RESP3	                    Simple	    _
// Booleans	        RESP3	                    Simple	    #
// Doubles	        RESP3	                    Simple	    ,
// Big numbers	    RESP3	                    Simple	    (
// Bulk errors	    RESP3	                    Aggregate	!
// Verbatim strings	RESP3	                    Aggregate	=
// Maps	            RESP3	                    Aggregate	%
// Sets	            RESP3	                    Aggregate	~
// Pushes	        RESP3	                    Aggregate	>
#[derive(Debug, PartialEq)]
pub enum ParseError {
    UnexpectedEndOfInput,
    InvalidByte, 
    InvalidFromUtf8(std::string::FromUtf8Error),
    InvalidUtf8(std::str::Utf8Error),
    InvalidInteger(std::num::ParseIntError),
    InvalidFloat(std::num::ParseFloatError),
    InvalidFloatConversion,
    InvalidLength,
    InvalidStringConversion
}

impl From<std::string::FromUtf8Error> for ParseError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        ParseError::InvalidFromUtf8(e)
    }
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(e: std::str::Utf8Error) -> Self {
        ParseError::InvalidUtf8(e)
    }
}

impl From<std::num::ParseIntError> for ParseError {
    fn from(e: std::num::ParseIntError) -> Self {
        ParseError::InvalidInteger(e)
    }
}

impl From<std::num::ParseFloatError> for ParseError {
    fn from(e: std::num::ParseFloatError) -> Self {
        ParseError::InvalidFloat(e)
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedEndOfInput => write!(f, "Unexpected end of input"),
            ParseError::InvalidByte => write!(f, "Invalid byte"),
            ParseError::InvalidFromUtf8(e) => write!(f, "Invalid from utf8: {}", e),
            ParseError::InvalidUtf8(e) => write!(f, "Invalid utf8: {}", e),
            ParseError::InvalidInteger(e) => write!(f, "Invalid integer: {}", e),
            ParseError::InvalidFloat(e) => write!(f, "Invalid float: {}", e),
            ParseError::InvalidFloatConversion => write!(f, "Invalid float conversion"),
            ParseError::InvalidLength => write!(f, "Invalid length"),
            ParseError::InvalidStringConversion => write!(f, "Invalid string conversion"),
        }
    }
}

impl Error for ParseError {}
// to-do - can we change this to be a copyless parser?
#[derive(Debug, Clone, PartialEq)]
pub enum Resp {
    // these are required to be utf8 encoded, hence they are strings instead of u8s
    SimpleString(String),
    // these are required to be utf8 encoded, hence they are strings instead of u8s
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    BulkStringNull,
    Array(Vec<Resp>),
    ArrayNull,
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(Vec<u8>), 
    BulkError(Vec<u8>),
    VerbatimString(Vec<u8>), // (encoding, content)
    Map(Vec<(Resp, Resp)>), // todo - could we implement hash for these types?
    Set(Vec<Resp>), // todo - could we implement hash for these types?
    Push(Vec<Resp>),
}

impl TryInto<String> for Resp {
    type Error = ParseError;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Resp::SimpleString(s) => Ok(s),
            Resp::SimpleError(s) => Ok(s),
            Resp::Integer(i) => Ok(i.to_string()), 
            Resp::BulkString(bytes) => Ok(String::from_utf8(bytes)?),
            Resp::BulkStringNull => Ok("".to_string()),
            Resp::ArrayNull => Ok("".to_string()),
            Resp::Boolean(b) => Ok(b.to_string()),
            Resp::Null => Ok("".to_string()),
            Resp::Double(f) => Ok(f.to_string()),
            Resp::VerbatimString(bytes) => Ok(String::from_utf8(bytes)?),
            _ => Err(ParseError::InvalidStringConversion),
        }
    }
}

impl Resp {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Resp::SimpleString(s) | Resp::SimpleError(s) => Some(s),
            Resp::BulkString(bytes) => std::str::from_utf8(bytes).ok(),
            Resp::BulkStringNull => Some(""),
            Resp::ArrayNull => Some(""),
            Resp::VerbatimString(bytes) => std::str::from_utf8(bytes).ok(),
            Resp::Null => Some(""),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Resp::SimpleString(s) | Resp::SimpleError(s) => Some(s.as_bytes()),
            Resp::BulkString(bytes) => Some(bytes),
            Resp::BulkError(bytes) => Some(bytes),
            Resp::VerbatimString(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Resp::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Resp::Double(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Resp::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_slice(&self) -> Option<&[Resp]> {
        match self {
            Resp::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn as_vec(self) -> Option<Vec<Resp>> {
        match self {
            Resp::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn is_bulk_string(&self) -> bool {
        matches!(self, Resp::BulkString(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Resp::Array(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Resp::Null)
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Resp::Integer(_))
    }

    pub fn is_simple_string(&self) -> bool {
        matches!(self, Resp::SimpleString(_))
    }

    pub fn is_simple_error(&self) -> bool {
        matches!(self, Resp::SimpleError(_))
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Resp::Boolean(_))
    }

    pub fn is_double(&self) -> bool {
        matches!(self, Resp::Double(_))
    }

    pub fn is_big_number(&self) -> bool {
        matches!(self, Resp::BigNumber(_))
    }

    pub fn is_bulk_error(&self) -> bool {
        matches!(self, Resp::BulkError(_))
    }

    pub fn is_verbatim_string(&self) -> bool {
        matches!(self, Resp::VerbatimString(_))
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Resp::Map(_))
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Resp::Set(_))
    }

    pub fn is_push(&self) -> bool {
        matches!(self, Resp::Push(_))
    }

    pub fn is_bulk_string_null(&self) -> bool {
        matches!(self, Resp::BulkStringNull)
    }

    pub fn is_array_null(&self) -> bool {
        matches!(self, Resp::ArrayNull)
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Resp::SimpleString(s) => s.is_empty(),
            Resp::SimpleError(s) => s.is_empty(),
            Resp::BulkString(bytes) => bytes.is_empty(),
            Resp::BulkError(bytes) => bytes.is_empty(),
            Resp::VerbatimString(bytes) => bytes.is_empty(),
            Resp::Array(arr) => arr.is_empty(),
            Resp::Map(m) => m.is_empty(),
            Resp::Set(s) => s.is_empty(),
            Resp::Push(p) => p.is_empty(),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct RespParser<'a> {
    data: &'a mut Cursor<BytesMut>,
}

impl<'a> RespParser<'a> { 
    pub fn new(data: &'a mut Cursor<BytesMut>) -> RespParser {
        RespParser { data }
    }

    pub fn is_eof(&self) -> bool {
        self.data.remaining() <= 0
    }

    pub fn check(&mut self) -> Result<Resp, ParseError> {
        let start_pos = self.data.position();
        let res = self.parse();
        self.data.set_position(start_pos);
        res
    }

    pub fn parse(&mut self) -> Result<Resp, ParseError> {
        self.parse_resp()
    }

    fn parse_resp(&mut self) -> Result<Resp, ParseError> {
        let first_byte = self.next_byte()?;

        match first_byte {
            b'+' => Ok(self.parse_simple_string()?),
            b'-' => Ok(self.parse_simple_error()?),
            b':' => Ok(self.parse_integer()?),
            b'$' => Ok(self.parse_bulk_string()?),
            b'*' => Ok(self.parse_array()?),
            b'_' => Ok(self.parse_null()?),
            b'#' => Ok(self.parse_boolean()?),
            b',' => Ok(self.parse_float()?),
            b'(' => Ok(self.parse_big_number()?),
            b'!' => Ok(self.parse_bulk_error()?),
            b'=' => Ok(self.parse_verbatim_string()?),
            b'%' => Ok(self.parse_map()?),
            b'~' => Ok(self.parse_set()?),
            b'>' => Ok(self.parse_push()?),
            _ => panic!("unsupported decoding type"),
        }
    }

    fn parse_simple_string(&mut self) -> Result<Resp, ParseError> {
        let result = self.parse_until_crlf()?;
        let string = String::from_utf8(result)?;
        Ok(Resp::SimpleString(string))
    }

    fn parse_simple_error(&mut self) -> Result<Resp, ParseError> {
        let result = self.parse_until_crlf()?;
        let string = String::from_utf8(result)?;
        Ok(Resp::SimpleError(string))
    }

    fn parse_integer(&mut self) -> Result<Resp, ParseError> {
        let result = self.parse_until_crlf()?;
        let string = std::str::from_utf8(&result)?;
        let integer = string.parse::<i64>()?;
        Ok(Resp::Integer(integer))
    }

    fn parse_bulk_string(&mut self) -> Result<Resp, ParseError> {
        let len_bytes = self.parse_until_crlf()?;
        let len = self.bytes_to_len(&len_bytes)?;
        
        if len < 0 {
            return Ok(Resp::BulkStringNull);
        }

        let result = self.vec_from_slice(len as usize)?;
        // note this is the only case were we need to manually advance
        // because we slice based on the len for effiency...
        self.expect_byte(b'\r')?;
        self.expect_byte(b'\n')?;
        // todo handle error case
        Ok(Resp::BulkString(result))
    }

    fn parse_array(&mut self) -> Result<Resp, ParseError> {
        let len_bytes = self.parse_until_crlf()?;
        let len = self.bytes_to_len(&len_bytes)?;
        
        if len < 0 {
            return Ok(Resp::ArrayNull);
        }

        if len == 0 {
            return Ok(Resp::Array(vec![]))
        }

        let mut res: Vec<Resp> = Vec::with_capacity(len as usize);

        for _ in 0..len {
            res.push(self.parse()?);
        }

        Ok(Resp::Array(res))
    }

    fn parse_null(&mut self) -> Result<Resp, ParseError> {
        let _ = self.parse_until_crlf()?;
        Ok(Resp::Null)
    }
    
    fn parse_boolean(&mut self) -> Result<Resp, ParseError> {
        match self.next_byte()? {
            b't' => Ok(Resp::Boolean(true)),
            b'f' => Ok(Resp::Boolean(false)),
            _ => Err(ParseError::InvalidByte),
        }
    }

    fn parse_float(&mut self) -> Result<Resp, ParseError> {
        let result = self.parse_until_crlf()?;
        let string = std::str::from_utf8(&result)?;
        match string {
            "inf" => Ok(Resp::Double(f64::INFINITY)),
            "-inf" => Ok(Resp::Double(f64::NEG_INFINITY)),
            "nan" => Ok(Resp::Double(f64::NAN)),
            other => {
                let float = other.parse::<f64>()?;

                if float.is_infinite() || float.is_nan() {
                    return Err(ParseError::InvalidFloatConversion);
                }

                Ok(Resp::Double(float))
            }
        }
    }

    fn parse_big_number(&mut self) -> Result<Resp, ParseError> {
        let result = self.parse_until_crlf()?;
        Ok(Resp::BigNumber(result))
    }

    fn parse_bulk_error(&mut self) -> Result<Resp, ParseError> {
        // this type has more or less the same impl as bulk string so will reuse...
        let bulk_str = self.parse_bulk_string()?; 
        match bulk_str {
            Resp::BulkString(s) => Ok(Resp::BulkError(s)),
            other => panic!("bulk error could not transform from {:#?}", other)
        }
    }

    fn parse_verbatim_string(&mut self) -> Result<Resp, ParseError> {
        // this type has more or less the same impl as bulk string so will reuse...
        let bulk_str = self.parse_bulk_string()?; 
        match bulk_str {
            Resp::BulkString(s) => Ok(Resp::VerbatimString(s)),
            _ => panic!("verbatim string could not transform from {:#?}", bulk_str)
        }
    }

    fn parse_map(&mut self) -> Result<Resp, ParseError> {
        let len_bytes = self.parse_until_crlf()?;
        let len = self.bytes_to_len(&len_bytes)?;
        if len < 0 {
            return Err(ParseError::InvalidLength);
        }

        let mut result: Vec<(Resp, Resp)> = Vec::with_capacity(len as usize);

        // todo - should we be validating these as we go?
        for _ in 0..len {
            let key = self.parse()?;
            let value = self.parse()?;
            result.push((key, value));
        }

        Ok(Resp::Map(result))
    }

    fn parse_set(&mut self) -> Result<Resp, ParseError> {
        let len_bytes = self.parse_until_crlf()?;
        let len = self.bytes_to_len(&len_bytes)?;

        if len < 0 {
            return Err(ParseError::InvalidLength);
        }

        let mut result: Vec<Resp> = Vec::with_capacity(len as usize);

        // todo - should we be validating these as we go?
        for _ in 0..len {
            let item = self.parse()?;
            result.push(item);
        }

        Ok(Resp::Set(result))
    }

    fn parse_push(&mut self) -> Result<Resp, ParseError> {
        // just parse an array and then return it as a push
        let result = self.parse_array()?;
        if let Resp::Array(arr) = result {
            return Ok(Resp::Push(arr));
        }
        unreachable!();
    }
    
    // note: this consumes the crlf character as well, so no need to check for it.
    fn parse_until_crlf(&mut self) -> Result<Vec<u8>, ParseError> {
        let mut result = Vec::new();
        loop {
            let byte = self.next_byte()?;
            if byte == b'\r' {
                self.expect_byte(b'\n')?;
                return Ok(result);
            }
            result.push(byte);
        }
    }

    fn bytes_to_len(&self, input: &[u8]) -> Result<i64, ParseError> {
        let len_str = std::str::from_utf8(input)?;
        let len = len_str.parse::<i64>()?;
        Ok(len)
    }

    fn next_byte(&mut self) -> Result<u8, ParseError> {
        if self.data.remaining() < 1 {
            return Err(ParseError::UnexpectedEndOfInput);
        }

        Ok(self.data.get_u8())
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), ParseError> {
        let byte = self.next_byte()?;

        if byte != expected {
            return Err(ParseError::InvalidByte);
        }

        Ok(())
    }

    fn get_slice(&mut self, to: usize) -> Result<&[u8], ParseError> {
        if self.data.remaining() < to {
            return Err(ParseError::UnexpectedEndOfInput)
        }

        let curr_pos = self.data.position() as usize;
        Ok(&self.data.get_ref()[curr_pos..curr_pos + to])
    }

    fn vec_from_slice(&mut self, to: usize) -> Result<Vec<u8>, ParseError> {
        let curr_pos = self.data.position();
        let mut result = Vec::with_capacity(to);
        result.extend_from_slice(self.get_slice(to)?);
        self.data.set_position(curr_pos + to as u64);
        Ok(result)
    }
}

pub struct RespEncoder;

impl RespEncoder {
    pub fn encode(data: &Resp) -> BytesMut {
        let mut buffer = BytesMut::new();
        Self::encode_resp(data, &mut buffer);
        buffer
    }

    pub fn encode_resp(data: &Resp, buffer: &mut BytesMut) {
        match data {
            Resp::SimpleString(s) => Self::encode_simple_string(s, buffer),
            Resp::SimpleError(s) => Self::encode_simple_error(s, buffer),
            Resp::Integer(i) => Self::encode_integer(*i, buffer),
            Resp::BulkString(bytes) => Self::encode_bulk_string(bytes, buffer),
            Resp::BulkStringNull => Self::encode_bulk_string_null(buffer),
            Resp::Array(arr) => Self::encode_array(arr, buffer),
            Resp::ArrayNull => Self::encode_array_null(buffer),
            Resp::Null => Self::encode_null(buffer),
            Resp::Boolean(b) => Self::encode_boolean(*b, buffer),
            Resp::Double(f) => Self::encode_double(*f, buffer),
            Resp::BigNumber(n) => Self::encode_big_number(n, buffer),
            Resp::BulkError(e) => Self::encode_bulk_error(e, buffer),
            Resp::VerbatimString(s) => Self::encode_verbatim_string(s, buffer),
            Resp::Map(m) => Self::encode_map(m, buffer),
            Resp::Set(s) => Self::encode_set(s, buffer),
            Resp::Push(p) => Self::encode_push(p, buffer),
        }
    }

    pub fn encode_simple_string(s: &str, buffer: &mut BytesMut) {
        buffer.put_u8(b'+');
        buffer.extend_from_slice(s.as_bytes());
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_simple_error(s: &str, buffer: &mut BytesMut) {
        buffer.put_u8(b'-');
        buffer.extend_from_slice(s.as_bytes());
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_integer(i: i64, buffer: &mut BytesMut) {
        buffer.put_u8(b':');
        buffer.extend_from_slice(i.to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_bulk_string(bytes: &[u8], buffer: &mut BytesMut) {
        buffer.put_u8(b'$');
        buffer.extend_from_slice(bytes.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(bytes);
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_bulk_string_null(buffer: &mut BytesMut) {
        buffer.put_u8(b'$');
        buffer.extend_from_slice(b"-1\r\n");
    }

    pub fn encode_array(arr: &[Resp], buffer: &mut BytesMut) {
        buffer.put_u8(b'*');
        buffer.extend_from_slice(arr.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        for item in arr {
            Self::encode_resp(item, buffer);
        }
    }

    pub fn encode_array_null(buffer: &mut BytesMut) {
        buffer.put_u8(b'*');
        buffer.extend_from_slice(b"-1\r\n");
    }

    pub fn encode_null(buffer: &mut BytesMut) {
        buffer.put_u8(b'_');
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_boolean(b: bool, buffer: &mut BytesMut) {
        buffer.put_u8(b'#');
        buffer.extend_from_slice(if b { b"t" } else { b"f" });
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_double(f: f64, buffer: &mut BytesMut) {
        buffer.put_u8(b',');

        if f.is_infinite() {
            if f.is_sign_positive() {
                buffer.extend_from_slice(b"inf");
            } else {
                buffer.extend_from_slice(b"-inf");
            }
        } else if f.is_nan() {
            buffer.extend_from_slice(b"nan");
        } else {
            buffer.extend_from_slice(format!("{:e}", f).as_bytes());
        }

        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_big_number(n: &[u8], buffer: &mut BytesMut) {
        buffer.put_u8(b'(');
        buffer.extend_from_slice(n);
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_bulk_error(e: &[u8], buffer: &mut BytesMut) {
        buffer.put_u8(b'!');
        buffer.extend_from_slice(e.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(e);
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_verbatim_string(s: &[u8], buffer: &mut BytesMut) {
        buffer.put_u8(b'=');
        buffer.extend_from_slice(s.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(s);
        buffer.extend_from_slice(b"\r\n");
    }

    pub fn encode_map(m: &[(Resp, Resp)], buffer: &mut BytesMut) {
        buffer.put_u8(b'%');
        buffer.extend_from_slice(m.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        for (key, value) in m {
            Self::encode_resp(key, buffer);
            Self::encode_resp(value, buffer);
        }
    }

    pub fn encode_set(s: &[Resp], buffer: &mut BytesMut) {
        buffer.put_u8(b'~');
        buffer.extend_from_slice(s.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        for item in s {
            Self::encode_resp(item, buffer);
        }
    }

    pub fn encode_push(p: &[Resp], buffer: &mut BytesMut) {
        buffer.put_u8(b'>');
        buffer.extend_from_slice(p.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        for item in p {
            Self::encode_resp(item, buffer);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let data = BytesMut::from(&b"+hello\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::SimpleString("hello".to_string()));
    }

    #[test]
    fn test_parse_simple_error() {
        let data = BytesMut::from(&b"-Error message\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::SimpleError("Error message".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let data = BytesMut::from(&b":123\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Integer(123));

        let data = BytesMut::from(&b":-123\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Integer(-123));
    }

    #[test]
    fn test_parse_bulk_string_null() {
        let data = BytesMut::from(&b"$-1\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::BulkStringNull);
    }

    #[test]
    fn test_parse_bulk_string() {
        let data = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn test_parse_bulk_string_empty() {
        let data = BytesMut::from(&b"$0\r\n\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::BulkString(b"".to_vec()));
    }

    #[test]
    fn test_parse_array() {
        let data = BytesMut::from(&b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor); 
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Array(vec![Resp::BulkString(b"hello".to_vec()), Resp::BulkString(b"world".to_vec())]));
    }

    #[test]
    fn test_parse_array_2() {
        let data = BytesMut::from(&b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Array(
            vec![
                Resp::Integer(1), 
                Resp::Integer(2), 
                Resp::Integer(3), 
                Resp::Integer(4), 
                Resp::BulkString(b"hello".to_vec())
            ]
        ));
    }

    #[test]
    fn test_parse_array_nested() {
        let data = BytesMut::from(&b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Array(
            vec![
                Resp::Array(
                    vec![
                        Resp::Integer(1),
                        Resp::Integer(2),
                        Resp::Integer(3) 
                    ]
                ),
                
                Resp::Array(
                    vec![
                        Resp::SimpleString("Hello".to_string()),
                        Resp::SimpleError("World".to_string())
                    ]
                )
            ]
        ));
    }

    #[test]
    fn test_parse_array_empty() {
        let data = BytesMut::from(&b"*0\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Array(vec![]));
    }

    #[test]
    fn test_parse_array_null() {
        let data = BytesMut::from(&b"*-1\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::ArrayNull);
    }

    #[test]
    fn test_parse_array_containing_null() {
        let data = BytesMut::from(&b"*2\r\n+Hello\r\n$-1\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Array(
            vec![
                Resp::SimpleString("Hello".to_string()),
                Resp::BulkStringNull
            ]
        ));
    }
    
    #[test]
    fn test_parse_null() {
        let data = BytesMut::from(&b"_\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Null);
    }

    #[test]
    fn test_parse_boolean_true() {
        let data = BytesMut::from(&b"#t\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Boolean(true));
    }

    #[test]
    fn test_parse_boolean_false() {
        let data = BytesMut::from(&b"#f\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Boolean(false));
    }

    #[test]
    fn test_parse_double() {
        let data = BytesMut::from(&b",1.23\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(1.23));
    }

    #[test]
    fn test_parse_double_negative() {
        let data = BytesMut::from(&b",-1.23\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(-1.23));
    }

    #[test]
    fn test_parse_double_int() {
        let data = BytesMut::from(&b",10\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(10.0));
    }

    #[test]
    fn test_parse_double_with_exponent() {
        let data = BytesMut::from(&b",1.23e-5\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(0.0000123));
    }

    #[test]
    fn test_parse_double_with_exponent_bigE() {
        let data = BytesMut::from(&b",1.23E-5\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(0.0000123));
    }

    #[test]
    fn test_parse_double_inf() {
        let data = BytesMut::from(&b",inf\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(f64::INFINITY));
    }

    #[test]
    fn test_parse_double_neg_inf() {
        let data = BytesMut::from(&b",-inf\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn test_parse_double_nan() {
        let data = BytesMut::from(&b",nan\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        // you can't make equality comparisons with nan directly.
        if let Resp::Double(nan) = result {
            assert_eq!(nan.to_string(), "NaN")
        }
    }

    #[test]
    fn test_parse_big_num() {
        let data = BytesMut::from(&b"(3492890328409238509324850943850943825024385\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::BigNumber(b"3492890328409238509324850943850943825024385".to_vec()));
    }

    #[test]
    fn test_parse_big_num_neg() {
        let data = BytesMut::from(&b"(-3492890328409238509324850943850943825024385\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::BigNumber(b"-3492890328409238509324850943850943825024385".to_vec()));
    }

    #[test]
    fn test_parse_bulk_err() {
        let data = BytesMut::from(&b"!21\r\nSYNTAX invalid syntax\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::BulkError(b"SYNTAX invalid syntax".to_vec()));
    }

    #[test]
    fn test_parse_verbatim_string() {
        let data = BytesMut::from(&b"=15\r\ntxt:Some string\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::VerbatimString(b"txt:Some string".to_vec()));
    }

    #[test]
    fn test_parse_map() {
        let data = BytesMut::from(&b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Map(
            vec![
                (Resp::SimpleString("first".to_string()), Resp::Integer(1)),
                (Resp::SimpleString("second".to_string()), Resp::Integer(2)),
            ]
        ));
    }

    #[test]
    fn test_parse_set() {
        let data = BytesMut::from(&b"~4\r\n+first\r\n:1\r\n+second\r\n:2\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Set(
            vec![
                Resp::SimpleString("first".to_string()), 
                Resp::Integer(1),
                Resp::SimpleString("second".to_string()), 
                Resp::Integer(2),
            ]
        ));
    }

    #[test]
    fn test_parse_push() {
        let data = BytesMut::from(&b">2\r\n+first\r\n:1\r\n"[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result = parser.parse().unwrap();
        assert_eq!(result, Resp::Push(
            vec![
                Resp::SimpleString("first".to_string()), 
                Resp::Integer(1),
            ]
        ));
    }

    #[test]
    fn test_parse_partial_stream() {
        let data = BytesMut::from(&b"+hello\r\n+world\r\n+partial..."[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result1 = parser.parse();
        let result2 = parser.parse();
        let result3 = parser.parse();
        let result4 = parser.parse();
        assert_eq!(result1.unwrap(), Resp::SimpleString("hello".to_string()));
        assert_eq!(result2.unwrap(), Resp::SimpleString("world".to_string()));
        assert_eq!(result3, Err(ParseError::UnexpectedEndOfInput));
        assert_eq!(result3, Err(ParseError::UnexpectedEndOfInput));
    }

    #[test]
    fn test_check_next_parse() {
        let data = BytesMut::from(&b"+hello\r\n+world\r\n+partial..."[..]);
        let mut cursor = Cursor::new(&data);
        let mut parser = RespParser::new(&mut cursor);
        let result1 = parser.check();
        assert_eq!(result1.unwrap(), Resp::SimpleString("hello".to_string()));
        assert_eq!(parser.data.position(), 0);
    }

    #[test]
    fn test_encode_simple_string() {
        let data = Resp::SimpleString("hello".to_string());
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"+hello\r\n");
    }

    #[test]
    fn test_encode_simple_error() {
        let data = Resp::SimpleError("Error message".to_string());
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"-Error message\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let data = Resp::Integer(123);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b":123\r\n");
    }

    #[test]
    fn test_encoder_integer_negative() {
        let data = Resp::Integer(-123);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b":-123\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let data = Resp::BulkString(b"hello".to_vec());
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_bulk_string_null() {
        let data = Resp::BulkStringNull;
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let data = Resp::Array(vec![Resp::BulkString(b"hello".to_vec()), Resp::BulkString(b"world".to_vec())]);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
    }

    #[test]
    fn test_encode_array_null() {
        let data = Resp::ArrayNull;
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"*-1\r\n");
    }

    #[test]
    fn test_encode_null() {
        let data = Resp::Null;
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"_\r\n");
    }

    #[test]
    fn test_encode_boolean_true() {
        let data = Resp::Boolean(true);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"#t\r\n");
    }

    #[test]
    fn test_encode_boolean_false() {
        let data = Resp::Boolean(false);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"#f\r\n");
    }

    #[test]
    fn test_encode_double() {
        let data = Resp::Double(1.23);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b",1.23e0\r\n");
    }

    #[test]
    fn test_encode_double_small() {
        let data = Resp::Double(0.0000123);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b",1.23e-5\r\n");
    }

    #[test]
    fn test_encode_double_inf() {
        let data = Resp::Double(f64::INFINITY);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b",inf\r\n");
    }

    #[test]
    fn test_encode_double_neg_inf() {
        let data = Resp::Double(f64::NEG_INFINITY);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b",-inf\r\n");
    }

    #[test]
    fn test_encode_double_nan() {
        let data = Resp::Double(f64::NAN);
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b",nan\r\n");
    }

    #[test]
    fn test_encode_big_number() {
        let data = Resp::BigNumber(b"3492890328409238509324850943850943825024385".to_vec());
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"(3492890328409238509324850943850943825024385\r\n");
    }

    #[test]
    fn test_encode_bulk_error() {
        let data = Resp::BulkError(b"SYNTAX invalid syntax".to_vec());
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"!21\r\nSYNTAX invalid syntax\r\n");
    }

    #[test]
    fn test_encode_verbatim_string() {
        let data = Resp::VerbatimString(b"txt:Some string".to_vec());
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"=15\r\ntxt:Some string\r\n");
    }

    #[test]
    fn test_encode_map() {
        let data = Resp::Map(
            vec![
                (Resp::SimpleString("first".to_string()), Resp::Integer(1)),
                (Resp::SimpleString("second".to_string()), Resp::Integer(2)),
            ]
        );
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
    }

    #[test]
    fn test_encode_set() {
        let data = Resp::Set(
            vec![
                Resp::SimpleString("first".to_string()), 
                Resp::Integer(1),
                Resp::SimpleString("second".to_string()), 
                Resp::Integer(2),
            ]
        );
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b"~4\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
    }

    #[test]
    fn test_encode_push() {
        let data = Resp::Push(
            vec![
                Resp::SimpleString("first".to_string()), 
                Resp::Integer(1),
            ]
        );
        let result = RespEncoder::encode(&data);
        assert_eq!(result.to_vec(), b">2\r\n+first\r\n:1\r\n");
    }
}
