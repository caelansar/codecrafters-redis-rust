mod cmd;
mod connection;
mod parse;
mod protocol;
mod rdb;
mod server;
mod storage;

use std::env;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let args = env::args().collect::<Vec<String>>();

    let port = args
        .iter()
        .position(|arg| arg == "--port")
        .and_then(|index| args.get(index + 1).cloned());
    let port: u16 = port.and_then(|p| p.parse().ok()).unwrap_or(6379);

    let addr = SocketAddr::from((Ipv4Addr::from_str("127.0.0.1").unwrap(), port));
    let listener = TcpListener::bind(addr).await.unwrap();

    let dir = args
        .iter()
        .position(|arg| arg == "--dir")
        .and_then(|index| args.get(index + 1).cloned());

    let db_filename = args
        .iter()
        .position(|arg| arg == "--dbfilename")
        .and_then(|index| args.get(index + 1).cloned());

    let replica_opt = args
        .iter()
        .position(|arg| arg == "--replicaof")
        .map(|index| {
            (
                args.get(index + 1).cloned(),
                args.get(index + 2).map(|x| x.parse::<u16>().unwrap()),
            )
        });

    server::run_server(listener, dir, db_filename, port, replica_opt).await
}
