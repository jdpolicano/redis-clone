// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use redis_starter_rust::resp::{ RespParser, Resp, RespEncoder};

#[tokio::main]
async fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = handle_stream(stream).await;
        });
    }
}

async fn handle_stream(mut stream: TcpStream) -> io::Result<()>  {
    let mut buffer = BytesMut::new();

    loop {
        let mut chunk = [0; 1024];
        let nbytes = stream.read(&mut chunk).await?;

        if nbytes == 0 {
            eprintln!("Connection closed");
            return Ok(());
        }

        buffer.extend_from_slice(&chunk[..nbytes]);
        eprintln!("Received: {}", String::from_utf8_lossy(&buffer[..nbytes]));

        let mut parser = RespParser::new(buffer.clone());

        if let Ok(cmd) = parser.parse() {
            eprintln!("Parsed: {:?}", cmd);
            handle_command(&mut stream, cmd).await?;
        } else {
            eprintln!("Failed to parse");
        }
    }
}

async fn handle_command(stream: &mut TcpStream, cmd: Resp) -> io::Result<()> {
    match cmd {
        Resp::Array(args_vec) => route_command(stream, args_vec).await,
        _ => {
            // minor overhead to create new string, but most of the time we won't hit this
            // because we'll be taking an in meory buffer copy of Resp...
            let mut buf = BytesMut::new();
            RespEncoder::encode_simple_string("ERR no array arguments", &mut buf);
            stream.write_all(&buf).await?;
            Ok(())
        }
    }
}

async fn route_command(stream: &mut TcpStream, args: Vec<Resp>) -> io::Result<()> {
    let mut arguments = Vec::new();
    for arg in args {
        match arg {
            Resp::BulkString(s) => {
                let as_str = String::from_utf8(s).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid UTF-8"))?;
                eprintln!("BulkString: {}", as_str);
                arguments.push(as_str);
            }
            _ => {
                eprintln!("Invalid argument: {:?}", arg);
                // return some kind of io related error...
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid argument type"))
            }
        }
    }

    if arguments.is_empty() {
        let mut buf = BytesMut::new();
        RespEncoder::encode_simple_string("ERR no command", &mut buf);
        stream.write_all(&buf).await?;
        return Ok(());
    }

    match arguments[0].to_lowercase().as_str() {
        "ping" => {
            let mut buf = BytesMut::new();
            RespEncoder::encode_simple_string("PONG", &mut buf);
            stream.write_all(&buf).await?;
        }
        "echo" => {
            let mut buf = BytesMut::new();
            let rest = arguments[1..].join(" ");
            RespEncoder::encode_bulk_string(rest.as_bytes(), &mut buf);
            stream.write_all(&buf).await?;
        }
        _ => { todo!("Implement the rest of the commands") }
    }
    Ok(())
}
// async fn handle_command(stream: &mut TcpStream, cmd: Resp) -> io::Result<()> {
//     let response = Resp::Array(vec![Resp::SimpleString("OK".to_string())]);
//     let mut encoder = RespEncoder::new();
//     let mut buffer = BytesMut::new();
//     encoder.encode(&response, &mut buffer).unwrap();
//     stream.write_all(&buffer).await?;
//     Ok(())
// }
