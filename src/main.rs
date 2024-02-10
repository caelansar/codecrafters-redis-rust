mod protocol;
mod store;

use crate::protocol::RESP;
use crate::store::Entry;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    map: Arc<Mutex<HashMap<String, Entry>>>,
    dir: Arc<Option<String>>,
    db_filename: Arc<Option<String>>,
) {
    println!("accepted new connection, addr {}", addr);

    let mut buf = [0; 512];
    while let Ok(_) = stream.read(&mut buf).await {
        let s = String::from_utf8_lossy(&buf);

        let resp: RESP = s.parse().unwrap();
        println!("recv command: {:?}", resp);

        if let RESP::Array(arr) = resp {
            if let Some(RESP::BulkString(Some(cmd))) = arr.get(0) {
                let resp = match cmd.to_lowercase().as_str() {
                    "echo" => arr.get(1).cloned().unwrap(),
                    "get" => {
                        let key = arr.get(1).unwrap();

                        if let RESP::BulkString(Some(key)) = key {
                            match map.lock().unwrap().get(key) {
                                Some(e) => {
                                    if e.exp.is_none() || e.exp.unwrap() > Instant::now() {
                                        RESP::BulkString(Some(e.val.clone()))
                                    } else {
                                        RESP::BulkString(None)
                                    }
                                }
                                None => RESP::BulkString(None),
                            }
                        } else {
                            unreachable!()
                        }
                    }
                    "set" => {
                        let key = arr.get(1).unwrap();
                        let val = arr.get(2).unwrap();

                        let px = arr.get(3);

                        if let RESP::BulkString(Some(key)) = key {
                            if let RESP::BulkString(Some(val)) = val {
                                let mut entry = Entry {
                                    val: val.to_string(),
                                    exp: None,
                                };
                                if px
                                    .is_some_and(|x| *x == RESP::BulkString(Some("px".to_string())))
                                {
                                    let exp = arr.get(4).unwrap();
                                    if let RESP::BulkString(Some(e)) = exp {
                                        let exp: u64 = e.parse().unwrap();
                                        entry.exp =
                                            Some(Instant::now().add(Duration::from_millis(exp)));
                                    }
                                }
                                map.lock().unwrap().insert(key.to_string(), entry);
                            }
                        }

                        RESP::SimpleString("OK".into())
                    }

                    "config" => {
                        let conf_name = arr.get(2).unwrap();
                        match conf_name {
                            RESP::BulkString(Some(x)) => {
                                if x == "dir" {
                                    RESP::Array(vec![
                                        RESP::BulkString(Some(x.clone())),
                                        RESP::BulkString(dir.as_ref().clone()),
                                    ])
                                } else if x == "dbfilename" {
                                    let db_filename = db_filename.clone();
                                    RESP::Array(vec![
                                        RESP::BulkString(Some(x.clone())),
                                        RESP::BulkString(db_filename.as_ref().clone()),
                                    ])
                                } else {
                                    unreachable!();
                                }
                            }
                            _ => unreachable!(),
                        }
                    }

                    _ => RESP::SimpleString("PONG".into()),
                };

                stream.write_all(resp.encode().as_bytes()).await.unwrap();
            } else {
                unreachable!()
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let map = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));

    let args = env::args().collect::<Vec<String>>();

    let dir = args
        .iter()
        .position(|arg| arg == "--dir")
        .and_then(|index| args.get(index + 1).cloned());

    let db_filename = args
        .iter()
        .position(|arg| arg == "--dbfilename")
        .and_then(|index| args.get(index + 1).cloned());

    println!("dir: {:?}, db_filename: {:?}", dir, db_filename);

    let dir = Arc::new(dir);
    let db_filename = Arc::new(db_filename);

    while let Ok((stream, addr)) = listener.accept().await {
        let map = map.clone();
        let dir = dir.clone();
        let db_filename = db_filename.clone();

        tokio::spawn(async move { handle_connection(stream, addr, map, dir, db_filename).await });
    }
}
