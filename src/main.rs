// Uncomment this block to pass the first stage
use std::net::TcpListener;
use std::io::Write;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_stream(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(mut stream: std::net::TcpStream) {
    // Your code here
    eprintln!("New connection from: {}", stream.peer_addr().unwrap());

    let message = "+PONG\r\n".as_bytes();

    if let Ok(_) = stream.write_all(message) {
        eprintln!("Sent: {}", String::from_utf8_lossy(message));
    } else {
        eprintln!("Failed to send response");
    }
}
