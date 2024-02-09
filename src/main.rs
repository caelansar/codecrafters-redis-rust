mod protocol;

use crate::protocol::RESP;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct Entry {
    val: String,
    exp: Option<Instant>,
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let map = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));

    while let Ok((mut stream, addr)) = listener.accept().await {
        println!("accepted new connection, addr {}", addr);

        let map = map.clone();

        tokio::spawn(async move {
            let mut buf = [0; 512];
            while let Ok(_) = stream.read(&mut buf).await {
                let s = String::from_utf8_lossy(&buf);

                let resp: RESP = s.parse().unwrap();
                println!("recv command: {:?}", resp);

                if let RESP::Array(arr) = resp {
                    if let Some(RESP::BulkString(Some(cmd))) = arr.get(0) {
                        match cmd.to_lowercase().as_str() {
                            "echo" => {
                                let data = arr.get(1).unwrap();
                                stream.write_all(data.encode().as_bytes()).await.unwrap();
                            }
                            "get" => {
                                let key = arr.get(1).unwrap();

                                if let RESP::BulkString(Some(key)) = key {
                                    let data = match map.lock().unwrap().get(key) {
                                        Some(e) => {
                                            if e.exp.is_none() || e.exp.unwrap() > Instant::now() {
                                                RESP::BulkString(Some(e.val.clone()))
                                            } else {
                                                RESP::BulkString(None)
                                            }
                                        }
                                        None => RESP::BulkString(None),
                                    };
                                    stream.write_all(data.encode().as_bytes()).await.unwrap();
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
                                        if px.is_some_and(|x| {
                                            *x == RESP::BulkString(Some("px".to_string()))
                                        }) {
                                            let exp = arr.get(4).unwrap();
                                            if let RESP::BulkString(Some(e)) = exp {
                                                let exp: u64 = e.parse().unwrap();
                                                entry.exp = Some(
                                                    Instant::now().add(Duration::from_millis(exp)),
                                                );
                                            }
                                        }
                                        map.lock().unwrap().insert(key.to_string(), entry);
                                    }
                                }

                                stream
                                    .write_all(RESP::SimpleString("OK".into()).encode().as_bytes())
                                    .await
                                    .unwrap();
                            }
                            _ => {
                                stream
                                    .write_all(
                                        RESP::SimpleString("PONG".into()).encode().as_bytes(),
                                    )
                                    .await
                                    .unwrap();
                            }
                        }
                    } else {
                        unreachable!()
                    }
                }
            }
        });
    }
}
