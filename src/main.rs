mod protocol;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    while let Ok(stream) = listener.accept().await {
        match stream {
            (mut stream, _) => {
                println!(
                    "accepted new connection, addr {}",
                    stream.peer_addr().unwrap()
                );

                tokio::spawn(async move {
                    let mut buf = [0; 512];
                    while let Ok(_) = stream.read(&mut buf).await {
                        stream.write_all("+PONG\r\n".as_bytes()).await.unwrap();
                    }
                });
            }
        }
    }
}
