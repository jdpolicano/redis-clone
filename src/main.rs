// Uncomment this block to pass the first stage
use std::net::TcpListener;
use std::io::{ Write, Read };

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                eprintln!("New connection from: {}", stream.peer_addr().unwrap());
                handle_stream(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(mut stream: std::net::TcpStream) -> std::io::Result<()> {
    loop {
        let mut buf = [0; 1024];
        let client_message = stream.read(&mut buf)?;

        if client_message == 0 {
            eprintln!("Connection closed");
            return Ok(());
        }

        eprintln!("Received: {}", String::from_utf8_lossy(&buf[..client_message]));
        let server_message = "+PONG\r\n".as_bytes();
        stream.write_all(server_message)?;
        eprintln!("Sent: {}", String::from_utf8_lossy(server_message));
    }
    
    Ok(())
}
