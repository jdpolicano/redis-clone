// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
use std::io::{ self };
use redis_starter_rust::resp::{ RespParser, Resp, RespEncoder};
use redis_starter_rust::database::{ Database, DbType };
use std::sync::Arc;


#[tokio::main]
async fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let mut database = Arc::new(Database::new());

    loop {
        let (stream, _) = listener.accept().await?;
        let db = database.clone();
        tokio::spawn(async move {
            let _ = handle_stream(stream, db).await;
        });
    }
}

async fn handle_stream(mut stream: TcpStream, db: Arc<Database>) -> io::Result<()>  {
    let mut buffer = BytesMut::new();

    loop {
        let mut chunk = [0; 1024];
        let nbytes = stream.read(&mut chunk).await?;

        if nbytes == 0 {
            return Ok(());
        }

        buffer.extend_from_slice(&chunk[..nbytes]);

        let mut parser = RespParser::new(buffer.clone());

        if let Ok(cmd) = parser.parse() {
            eprintln!("Parsed: {:?}", cmd);
            match handle_command(&mut stream, cmd, db.clone()).await {
                Ok(_) => {
                    buffer.clear();
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    buffer.clear();
                    return Err(e);
                }
            }
        } else {
            eprintln!("Failed to parse");
        }
    }
}

async fn handle_command(stream: &mut TcpStream, cmd: Resp, db: Arc<Database>) -> io::Result<()> {
    match cmd {
        Resp::Array(args_vec) => route_command(stream, args_vec, db).await,
        _ => Err(invalid_input("ERR invalid client type, expected Array"))
    }
}

async fn route_command(stream: &mut TcpStream, mut cmd_args: Vec<Resp>, db: Arc<Database>) -> io::Result<()>  {
    // reverse the order of the arguments
    // now order is [arg3 arg2 arg1 command]
    cmd_args.reverse();
    let last = cmd_args
        .pop()
        .ok_or("ERR No command".to_string())
        .map_err(|e| invalid_input(&e.to_string()))?;

    let command = bulk_to_string(last)?;

    match command.to_lowercase().as_str() {
        "ping" => {
            let mut buf = BytesMut::new();
            RespEncoder::encode_simple_string("PONG", &mut buf);
            stream.write_all(&buf).await?;
        }
        "echo" => {
            let mut buf = BytesMut::new();
            let rest = cmd_args
                .pop()
                .ok_or(invalid_input("ERR No echo value"))?;

            RespEncoder::encode_resp(&rest, &mut buf);
            stream.write_all(&buf).await?;
        }
        "set" => {
            // take the first two elements for now...
            let (key, value) = (cmd_args.pop(), cmd_args.pop());

            if key.is_none() || value.is_none() {
                return Err(invalid_input("key or value missing"))
            }

            let string_key = DbType::from_resp(key.unwrap()).ok_or(invalid_input("ERR invalid key type"))?;
            let value = DbType::from_resp(value.unwrap()).ok_or(invalid_input("ERR invalid value type"))?;
            
            db.set(string_key, value);

            let mut buf = BytesMut::new();
            RespEncoder::encode_simple_string("OK", &mut buf);
            stream.write_all(&buf).await?;
        }

        "get" => {
            let key = cmd_args.pop().ok_or(invalid_input("ERR No key"))?;
            let string_key = DbType::from_resp(key).ok_or(invalid_input("ERR invalid key type"))?;
            let value = db.get(&string_key).unwrap_or(DbType::String(Vec::new()));

            let mut buf = BytesMut::new();
            RespEncoder::encode_bulk_string(&value.to_bulk_str().unwrap(), &mut buf);
            stream.write_all(&buf).await?;
        }
        _ => { todo!("Implement the rest of the commands") }
    }
    return Ok(());
} 

fn bulk_to_string(resp: Resp) -> io::Result<String> {
    if let Resp::BulkString(bytes) = resp {
        return String::from_utf8(bytes).map_err(|e| invalid_input(&e.to_string()));
    } else {
        return Err(invalid_input("ERR unexpected RESP type"))
    }
}

fn invalid_input(e: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, e)
}
// async fn handle_command(stream: &mut TcpStream, cmd: Resp) -> io::Result<()> {
//     let response = Resp::Array(vec![Resp::SimpleString("OK".to_string())]);
//     let mut encoder = RespEncoder::new();
//     let mut buffer = BytesMut::new();
//     encoder.encode(&response, &mut buffer).unwrap();
//     stream.write_all(&buffer).await?;
//     Ok(())
// }
