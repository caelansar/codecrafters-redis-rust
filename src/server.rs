use crate::cmd::Command;
use crate::connection::Connection;
use crate::protocol::RESP;
use crate::rdb::consts;
use crate::rdb::parser::Parser;
use crate::storage::Entry;
use anyhow::Context;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::ops::{Add, Sub};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;

pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

async fn handle_connection(
    streams: (OwnedReadHalf, OwnedWriteHalf),
    addr: SocketAddr,
    db: Arc<Mutex<HashMap<String, Entry>>>,
    dir: Arc<Option<String>>,
    db_filename: Arc<Option<String>>,
    replica_opt: Arc<Option<(Option<String>, Option<u16>)>>,
    replicas: Arc<Mutex<Vec<Arc<Mutex<OwnedWriteHalf>>>>>,
    tx: Sender<RESP>,
    master_offset: Arc<AtomicUsize>,
    ack: Arc<AtomicUsize>,
) {
    println!("accepted new connection, addr {}", addr);

    let stream = streams.0;
    let mut conn = Connection::new(stream);

    let writer = Arc::new(Mutex::new(streams.1));

    while let Ok(Some(resp)) = conn.read_frame().await {
        println!("recv command: {:?}", resp);

        let cmd = Command::from_resp_frame(resp).unwrap();
        match cmd {
            Command::Get(get) => {
                // TODO: remove
                // block command to let it propagate to replicas
                tokio::time::sleep(Duration::from_millis(20)).await;

                let resp = match db.lock().await.get(get.key()) {
                    Some(e) => match e.exp {
                        None => Some(RESP::BulkString(Some(e.val.clone()))),
                        Some(exp) if exp > SystemTime::now() => {
                            Some(RESP::BulkString(Some(e.val.clone())))
                        }
                        _ => Some(RESP::BulkString(None)),
                    },
                    None => Some(RESP::BulkString(None)),
                };
                if let Some(resp) = resp {
                    writer
                        .lock()
                        .await
                        .write_all(resp.encode().as_bytes())
                        .await
                        .unwrap();
                }
            }
            Command::Set(set) => {
                // master node, propagate SET command
                let key = set.key().to_string();
                let val = String::from_utf8_lossy(set.value()).to_string();
                let expire = set.expire();

                if replica_opt.is_none() {
                    let data = set.into_frame();

                    master_offset.fetch_add(data.encode().len(), Ordering::Relaxed);

                    tx.send(data).await.unwrap();
                }

                let mut entry = Entry { val, exp: None };

                if expire.is_some() {
                    entry.exp = Some(SystemTime::now().add(expire.unwrap()));
                }

                db.lock().await.insert(key, entry);

                let resp = Some(RESP::SimpleString("OK".into()));

                if let Some(resp) = resp {
                    writer
                        .lock()
                        .await
                        .write_all(resp.encode().as_bytes())
                        .await
                        .unwrap();
                }
            }
            Command::Raw(resp) => {
                if let RESP::Array(arr) = resp {
                    if let Some(RESP::BulkString(Some(cmd))) = arr.first() {
                        let resp = match cmd.to_lowercase().as_str() {
                            "echo" => arr.get(1).cloned(),
                            "config" => {
                                let conf_name = arr.get(2).unwrap();
                                match conf_name {
                                    RESP::BulkString(Some(x)) => {
                                        if x == "dir" {
                                            Some(RESP::Array(vec![
                                                RESP::BulkString(Some(x.clone())),
                                                RESP::BulkString(dir.as_ref().clone()),
                                            ]))
                                        } else if x == "dbfilename" {
                                            let db_filename = db_filename.clone();
                                            Some(RESP::Array(vec![
                                                RESP::BulkString(Some(x.clone())),
                                                RESP::BulkString(db_filename.as_ref().clone()),
                                            ]))
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

                                Some(RESP::Array(arr))
                            }

                            "replconf" => {
                                let param = arr.get(1).unwrap();
                                if let RESP::BulkString(Some(s)) = param {
                                    // return from replicas
                                    if s.to_lowercase() == "ack" {
                                        let offset = arr.get(2).unwrap();
                                        if let RESP::BulkString(Some(offset)) = offset {
                                            let offset: usize = offset.parse().unwrap();
                                            let master_offset =
                                                master_offset.load(Ordering::Relaxed);
                                            println!(
                                                "getack offset: {}, master offset: {}",
                                                offset, master_offset
                                            );

                                            if offset > master_offset {
                                                ack.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                        None
                                    } else {
                                        Some(RESP::SimpleString("OK".into()))
                                    }
                                } else {
                                    unreachable!()
                                }
                            }

                            "wait" => {
                                if replica_opt.is_none() {
                                    let start = Instant::now();

                                    let number = arr
                                        .get(1)
                                        .and_then(|x| {
                                            if let RESP::BulkString(Some(s)) = x {
                                                s.parse::<usize>().ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap();
                                    let timeout = arr
                                        .get(2)
                                        .and_then(|x| {
                                            if let RESP::BulkString(Some(s)) = x {
                                                s.parse::<u64>().ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap();

                                    let master_offset = master_offset.load(Ordering::Relaxed);
                                    println!(
                                        "wait, number: {}, timeout: {}, master offset: {}",
                                        number, timeout, master_offset
                                    );

                                    let mut reply = ack.load(Ordering::Relaxed);

                                    if master_offset == 0 {
                                        reply = replicas.lock().await.len();
                                    } else {
                                        for replica in replicas.lock().await.iter() {
                                            println!("send REPLCONF GETACK");
                                            let resp = RESP::Array(vec![
                                                RESP::BulkString(Some("REPLCONF".into())),
                                                RESP::BulkString(Some("GETACK".into())),
                                                RESP::BulkString(Some("*".into())),
                                            ]);
                                            replica
                                                .lock()
                                                .await
                                                .write_all(resp.encode().as_bytes())
                                                .await
                                                .unwrap();
                                        }

                                        let elapse = start.elapsed();

                                        if reply < number {
                                            println!("wait timeout");
                                            tokio::time::sleep(
                                                Duration::from_millis(timeout).sub(elapse),
                                            )
                                            .await;
                                            // fetch again
                                            reply = ack.load(Ordering::Relaxed);
                                        }

                                        // reset to zero
                                        ack.store(0, Ordering::Relaxed);
                                    }

                                    println!("get reply: {}, wait number: {}", reply, number);
                                    Some(RESP::Integer(reply.min(number) as i64))
                                } else {
                                    None
                                }
                            }

                            "psync" => Some(RESP::SimpleString(
                                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".into(),
                            )),

                            "info" => {
                                if replica_opt.is_none() {
                                    Some(RESP::BulkString(Some("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0".into())))
                                } else {
                                    Some(RESP::BulkString(Some("role:slave".into())))
                                }
                            }

                            "ping" => Some(RESP::SimpleString("PONG".into())),

                            _ => unimplemented!(),
                        };

                        if let Some(resp) = resp {
                            writer
                                .lock()
                                .await
                                .write_all(resp.encode().as_bytes())
                                .await
                                .unwrap();
                        }

                        if cmd.to_lowercase() == "psync" {
                            println!("send rdb");
                            let binary_rdb = decode_hex(consts::EMPTY_RDB).unwrap();
                            let mut data = format!("${}\r\n", binary_rdb.len()).into_bytes();
                            data.extend(binary_rdb);
                            writer.lock().await.write_all(&data).await.unwrap();

                            replicas.lock().await.push(writer.clone());
                            println!("push replicas")
                        }
                    } else {
                        unreachable!()
                    }
                }
            }
        }
    }
}
pub async fn run_server(
    listener: TcpListener,
    dir: Option<String>,
    db_filename: Option<String>,
    port: u16,
    replica_opt: Option<(Option<String>, Option<u16>)>,
) {
    let replica_opt = Arc::new(replica_opt);

    println!(
        "dir: {:?}, db_filename: {:?}, replica_opt: {:?}",
        dir,
        db_filename,
        replica_opt.as_ref()
    );

    let db = init_db(&dir, &db_filename).await;
    let db = Arc::new(Mutex::new(db));

    let dir = Arc::new(dir);
    let db_filename = Arc::new(db_filename);

    handshake(port, replica_opt.clone(), db.clone())
        .await
        .unwrap();

    let replicas: Arc<Mutex<Vec<Arc<Mutex<OwnedWriteHalf>>>>> = Arc::new(Mutex::new(vec![]));
    let replicas_clone = Arc::clone(&replicas);

    let (tx, mut rx) = mpsc::channel::<RESP>(10);

    tokio::spawn(async move {
        while let Some(resp) = rx.recv().await {
            println!(
                "replica recv command: {:?}, replicas len: {}",
                resp,
                replicas.lock().await.len()
            );
            for replica in replicas.lock().await.iter() {
                println!("prepare send");
                replica
                    .lock()
                    .await
                    .write_all(resp.encode().as_bytes())
                    .await
                    .context("failed to propagate")
                    .unwrap();
            }
        }
    });

    let answer = Arc::new(AtomicUsize::new(0));
    let master_offset = Arc::new(AtomicUsize::new(0));

    while let Ok((stream, addr)) = listener.accept().await {
        let db = db.clone();
        let dir = dir.clone();
        let db_filename = db_filename.clone();
        let replica_opt = replica_opt.clone();
        let replicas_clone = Arc::clone(&replicas_clone);
        let tx = tx.clone();

        let stream = stream.into_split();
        let answer = answer.clone();
        let master_offset = master_offset.clone();

        tokio::spawn(async move {
            handle_connection(
                stream,
                addr,
                db,
                dir,
                db_filename,
                replica_opt,
                replicas_clone,
                tx,
                master_offset,
                answer,
            )
            .await
        });
    }
}

async fn init_db(dir: &Option<String>, db_filename: &Option<String>) -> HashMap<String, Entry> {
    if let (Some(dir), Some(db_filename)) = (dir, db_filename) {
        let file = File::open(&Path::new(dir).join(db_filename)).await;

        match file {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                println!("{}/{} not found", dir, db_filename);
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

async fn handshake(
    listening_port: u16,
    replica_opt: Arc<Option<(Option<String>, Option<u16>)>>,
    db: Arc<Mutex<HashMap<String, Entry>>>,
) -> anyhow::Result<()> {
    if let Some((Some(host), Some(port))) = replica_opt.as_ref() {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let (reader, mut writer) = stream.into_split();
        let mut conn = Connection::new(reader);

        // The replica sends a PING to the master
        let resp = RESP::Array(vec![RESP::BulkString(Some("PING".into()))]);
        writer.write_all(resp.encode().as_bytes()).await?;

        // The replica sends REPLCONF twice to the master
        let resp1 = RESP::Array(vec![
            RESP::BulkString(Some("REPLCONF".into())),
            RESP::BulkString(Some("listening-port".into())),
            RESP::BulkString(Some(listening_port.to_string())),
        ]);

        let resp2 = RESP::Array(vec![
            RESP::BulkString(Some("REPLCONF".into())),
            RESP::BulkString(Some("capa".into())),
            RESP::BulkString(Some("psync2".into())),
        ]);

        let mut data = resp1.encode().into_bytes();
        data.extend_from_slice(resp2.encode().as_bytes());
        writer.write_all(&data).await?;

        let resp = conn.read_frame().await?;
        assert_eq!(Some(RESP::SimpleString("PONG".into())), resp);

        let resp = conn.read_frame().await?;
        assert_eq!(Some(RESP::SimpleString("OK".into())), resp);

        let resp = conn.read_frame().await?;
        assert_eq!(Some(RESP::SimpleString("OK".into())), resp);

        let resp = RESP::Array(vec![
            RESP::BulkString(Some("PSYNC".into())),
            RESP::BulkString(Some("?".into())),
            RESP::BulkString(Some("-1".into())),
        ]);
        writer.write_all(resp.encode().as_bytes()).await?;

        println!("PSYNC sent");

        let resp = conn.read_frame().await?;
        assert!(matches!(resp, Some(RESP::SimpleString(_))));

        println!("recv FULLRESYNC");

        // recv empty rdb
        let n = conn.skip_rdb().await.context("failed to read rdb")?;
        println!("receive RDB file, n: {}", n);
        if n == 0 {
            return Ok(());
        }

        tokio::spawn(async move {
            println!("handle propagated commands from master");
            let mut offset: usize = 0;
            while let Ok(Some(resp)) = conn.read_frame().await {
                let mut db_guard = db.lock().await;
                println!("replica receive command from master: {:?}", resp);
                if let RESP::Array(ref arr) = resp {
                    if let Some(RESP::BulkString(Some(cmd))) = arr.first() {
                        match cmd.to_lowercase().as_str() {
                            "replconf" => {
                                drop(db_guard);

                                let resp = RESP::Array(vec![
                                    RESP::BulkString(Some("REPLCONF".into())),
                                    RESP::BulkString(Some("ACK".into())),
                                    RESP::BulkString(Some(offset.to_string())),
                                ]);

                                let data = resp.encode();
                                println!("response: {:?}", data);
                                writer.write_all(data.as_bytes()).await.unwrap();
                            }
                            "ping" => {
                                drop(db_guard);
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
                                                    SystemTime::now()
                                                        .add(Duration::from_millis(exp)),
                                                );
                                            }
                                        }
                                        db_guard.insert(key.to_string(), entry);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        };
                        offset += resp.encode().as_bytes().len();
                    }
                }
            }
        });
    }

    Ok(())
}
