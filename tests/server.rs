use redis_starter_rust::server::run_server;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A server instance is started in a background task. A client TCP
/// connection is then established and raw redis commands are sent to
/// the server. The response is evaluated at the byte level.
#[tokio::test]
async fn key_value_get_set() {
    let addr = start_server().await;

    // Establish a connection to the server
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Get a key, data is missing
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // Read nil response
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);

    // Set a key
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await
        .unwrap();

    // Read OK
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    // Get the key, data is present
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // Shutdown the write half
    stream.shutdown().await.unwrap();

    // Read "world" response
    let mut response = [0; 11];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$5\r\nworld\r\n", &response);

    // Receive `None`
    assert_eq!(0, stream.read(&mut response).await.unwrap());
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { run_server(listener, None, None, 0, None).await });

    addr
}
