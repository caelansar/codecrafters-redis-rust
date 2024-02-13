mod protocol;
mod rdb;
mod storage;

use crate::protocol::RESP;
use crate::rdb::parser::Parser;
use crate::storage::Entry;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Add;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{env, io};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    db: Arc<Mutex<HashMap<String, Entry>>>,
    dir: Arc<Option<String>>,
    db_filename: Arc<Option<String>>,
    is_master: bool,
) {
    println!("accepted new connection, addr {}", addr);

    let mut buf = [0; 512];
    while stream.read(&mut buf).await.is_ok() {
        let s = String::from_utf8_lossy(&buf);

        let resp: RESP = s.parse().unwrap();
        println!("recv command: {:?}", resp);

        if let RESP::Array(arr) = resp {
            if let Some(RESP::BulkString(Some(cmd))) = arr.first() {
                let resp = match cmd.to_lowercase().as_str() {
                    "echo" => arr.get(1).cloned().unwrap(),
                    "get" => {
                        let key = arr.get(1).unwrap();

                        if let RESP::BulkString(Some(key)) = key {
                            match db.lock().await.get(key) {
                                Some(e) => {
                                    if e.exp.is_none() || e.exp.unwrap() > SystemTime::now() {
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
                                            Some(SystemTime::now().add(Duration::from_millis(exp)));
                                    }
                                }
                                db.lock().await.insert(key.to_string(), entry);
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

                    "keys" => {
                        let arr = db
                            .lock()
                            .await
                            .keys()
                            .map(|x| RESP::BulkString(Some(x.clone())))
                            .collect::<Vec<RESP>>();

                        RESP::Array(arr)
                    }

                    "info" => {
                        if is_master {
                            RESP::BulkString(Some("role:master".into()))
                        } else {
                            RESP::BulkString(Some("role:slave".into()))
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

async fn init_db(dir: &Option<String>, db_filename: &Option<String>) -> HashMap<String, Entry> {
    if let (Some(dir), Some(dbfilename)) = (dir, db_filename) {
        let file = File::open(&Path::new(dir).join(dbfilename)).await;

        match file {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                println!("{}/{} not found", dir, dbfilename);
                HashMap::new()
            }
            Err(e) => panic!("failed to read rdb file: {}", &e),
            Ok(_) => {
                let file = file.unwrap();
                let reader = BufReader::new(file);

                let mut parser = Parser::new(reader);
                parser.parse().await.unwrap();

                parser
                    .get_kv_pairs()
                    .filter_map(|(k, (v, exp))| {
                        if let &RESP::BulkString(Some(ref x)) = v {
                            match exp {
                                Some(exp) if *exp > SystemTime::now() => Some((
                                    k.clone(),
                                    Entry {
                                        val: x.clone(),
                                        exp: Some(*exp),
                                    },
                                )),
                                Some(_) => None, // expired
                                None => Some((
                                    k.clone(),
                                    Entry {
                                        val: x.clone(),
                                        exp: None,
                                    },
                                )),
                            }
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        }
    } else {
        HashMap::new()
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let args = env::args().collect::<Vec<String>>();

    let port = args
        .iter()
        .position(|arg| arg == "--port")
        .and_then(|index| args.get(index + 1).cloned());
    let port: Option<u16> = port.and_then(|p| p.parse().ok());

    let addr = SocketAddr::from((
        Ipv4Addr::from_str("127.0.0.1").unwrap(),
        port.unwrap_or(6379),
    ));
    let listener = TcpListener::bind(addr).await.unwrap();

    let dir = args
        .iter()
        .position(|arg| arg == "--dir")
        .and_then(|index| args.get(index + 1).cloned());

    let db_filename = args
        .iter()
        .position(|arg| arg == "--dbfilename")
        .and_then(|index| args.get(index + 1).cloned());

    println!("dir: {:?}, db_filename: {:?}", dir, db_filename);

    let db = init_db(&dir, &db_filename).await;
    let db = Arc::new(Mutex::new(db));

    let dir = Arc::new(dir);
    let db_filename = Arc::new(db_filename);

    while let Ok((stream, addr)) = listener.accept().await {
        let db = db.clone();
        let dir = dir.clone();
        let db_filename = db_filename.clone();

        tokio::spawn(
            async move { handle_connection(stream, addr, db, dir, db_filename, true).await },
        );
    }
}
