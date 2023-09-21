use std::{io::Write, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!(
                    "accepted new connection, addr {}",
                    stream.peer_addr().unwrap()
                );
                loop {
                    if let Err(_) = stream.write_all(b"+PONG\r\n") {
                        break;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
