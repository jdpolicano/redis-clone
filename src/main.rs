// Uncomment this block to pass the first stage
use std::io::{ self };
use redis_starter_rust::server::{ RedisServer, ServerArguments };

#[tokio::main]
async fn main() -> io::Result<()> {
    let server_args = ServerArguments::parse();
    let server = RedisServer::bind(server_args).await?;
    server.listener.run().await?;
    Ok(())
}

