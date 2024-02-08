mod protocol;

use crate::protocol::RESP;
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
                        let s = String::from_utf8_lossy(&buf);

                        let resp: RESP = s.parse().unwrap();
                        println!("recv command: {:?}", resp);

                        if let RESP::Array(arr) = resp {
                            if let Some(RESP::BulkString(cmd)) = arr.get(0) {
                                if cmd.to_lowercase() == "echo" {
                                    let data = arr.get(1).unwrap();
                                    stream.write_all(data.encode().as_bytes()).await.unwrap();
                                } else {
                                    stream.write_all("+PONG\r\n".as_bytes()).await.unwrap();
                                }
                            } else {
                                unreachable!()
                            }
                        }
                    }
                });
            }
        }
    }
}
