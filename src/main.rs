// Uncomment this block to pass the first stage
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::{ self, Write, Read };

#[tokio::main]
async fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_stream(stream).await;
        });
    }

    Ok(())
}

async fn handle_stream(mut stream: TcpStream) -> io::Result<()>  {
    loop {
        let mut buf = [0; 1024];
        let client_message = stream.read(&mut buf).await?;

        if client_message == 0 {
            eprintln!("Connection closed");
            return Ok(());
        }

        eprintln!("Received: {}", String::from_utf8_lossy(&buf[..client_message]));
        let server_message = "+PONG\r\n".as_bytes();
        stream.write_all(server_message).await?;
        eprintln!("Sent: {}", String::from_utf8_lossy(server_message));
    }
}
