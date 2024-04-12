use crate::cmd::time_spec::TimeSepc;
use crate::cmd::Command;
use crate::connection::Connection;
use crate::protocol::RESP;
use crate::rdb::consts;
use crate::rdb::parser::Parser;
use crate::storage::Db;
use anyhow::Context;
use bytes::Bytes;
use std::io;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::ops::Sub;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::time;
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
    db: Db,
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
            Command::Echo(echo) => echo.apply(writer.clone()).await.unwrap(),
            Command::Keys(keys) => keys.apply(&db, writer.clone()).await.unwrap(),
            Command::Ping(ping) => ping.apply(writer.clone()).await.unwrap(),
            Command::Subscribe(subscribe) => subscribe
                .apply(&db, &mut conn, writer.clone())
                .await
                .unwrap(),
            Command::Unsubscribe(_) => unreachable!(),
            Command::Publish(publish) => publish.apply(&db, writer.clone()).await.unwrap(),
            Command::XRead(xread) => {
                let pairs = xread.key_start_pairs();

                let mut streams = Vec::new();

                if let Some(duration) = xread.block() {
                    println!("block {}ms", duration.as_millis());
                    if duration.as_millis() == 0 {
                        for (key, _) in &pairs {
                            let mut arr = Vec::new();
                            arr.push(RESP::BulkString(Bytes::from(key.to_string())));

                            let rx = db.block_stream(key.to_string());

                            let stream = rx.await.unwrap();
                            println!("recv stream: {:?}", stream);
                            arr.push(RESP::Array(vec![stream]));

                            streams.push(RESP::Array(arr));
                        }

                        writer
                            .lock()
                            .await
                            .write_all(RESP::Array(streams).encode().as_bytes())
                            .await
                            .unwrap();

                        continue;
                    }
                    time::sleep(duration).await
                }

                pairs.into_iter().for_each(|(key, start)| {
                    let mut arr = Vec::new();
                    arr.push(RESP::BulkString(Bytes::from(key.to_string())));

                    let arr = match db.get_stream(key) {
                        Some(stream) => {
                            let iter = stream.into_iter().filter(|(id, _)| {
                                let t: TimeSepc = id.parse().unwrap();
                                &t > start
                            });

                            let mut streams = Vec::new();

                            iter.for_each(|(id, data)| {
                                let items = vec![RESP::BulkString(Bytes::from(id)), data.into()];
                                streams.push(RESP::Array(items));
                            });

                            if streams.is_empty() {
                                arr.pop();
                                arr
                            } else {
                                arr.push(RESP::Array(streams));
                                arr
                            }
                        }
                        None => {
                            // remove stream key in arr
                            arr.pop();
                            arr
                        }
                    };

                    if !arr.is_empty() {
                        let resp = RESP::Array(arr);
                        streams.push(resp);
                    }
                });

                let resp = if streams.is_empty() {
                    RESP::Null
                } else {
                    RESP::Array(streams)
                };

                writer
                    .lock()
                    .await
                    .write_all(resp.encode().as_bytes())
                    .await
                    .unwrap();
            }
            Command::XRange(xrange) => {
                use std::ops::RangeBounds;

                let key = xrange.key();

                let mut arr = Vec::new();
                let resp = match db.get_stream(key) {
                    Some(stream) => {
                        let iter = stream.into_iter().filter(|(id, _)| {
                            let t = id.parse().unwrap();
                            xrange.range().contains(&t)
                        });
                        iter.for_each(|(id, data)| {
                            let items = vec![RESP::BulkString(Bytes::from(id)), data.into()];
                            arr.push(RESP::Array(items));
                        });

                        RESP::Array(arr)
                    }
                    None => RESP::Array(arr),
                };

                writer
                    .lock()
                    .await
                    .write_all(resp.encode().as_bytes())
                    .await
                    .unwrap();
            }
            Command::Xadd(xadd) => {
                let mut id = xadd.id().to_string();

                match id.as_str() {
                    "*" => {
                        let now = SystemTime::now();
                        let duration = now.duration_since(UNIX_EPOCH).unwrap();

                        id = format!("{}-{}", duration.as_millis(), 0);
                    }
                    _ => {
                        let parts = id.split_once('-');
                        if let Some((time, seq)) = parts {
                            if seq == "*" {
                                match db.get_stream(xadd.key()) {
                                    None => {
                                        if time == "0" {
                                            id = format!("{}-{}", time, 1);
                                        } else {
                                            id = format!("{}-{}", time, 0);
                                        }
                                    }
                                    Some(stream) => {
                                        let next_seq = stream
                                            .iter()
                                            .filter_map(|(s, _)| {
                                                let (t, s) = s.split_once('-').unwrap();
                                                if t == time {
                                                    Some((t, s))
                                                } else {
                                                    None
                                                }
                                            })
                                            .last()
                                            .map(|(_, seq)| {
                                                let seq: u64 = seq.parse().unwrap();
                                                seq + 1
                                            });
                                        if next_seq.is_some() {
                                            println!("next seq: {:?}", next_seq);

                                            id = format!("{}-{}", time, next_seq.unwrap());
                                        } else {
                                            id = format!("{}-{}", time, 0);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let resp = match db.set_stream(xadd.key(), id.clone(), xadd.data()) {
                    Ok(_) => RESP::BulkString(Bytes::from(id.clone())),
                    Err(e) => RESP::Error(e.to_string()),
                };

                let senders = db.get_all_block(xadd.key());
                senders
                    .into_iter()
                    .for_each(|sender| sender.send(RESP::Array(xadd.to_item())).unwrap());

                writer
                    .lock()
                    .await
                    .write_all(resp.encode().as_bytes())
                    .await
                    .unwrap();
            }
            Command::Type(typ) => typ.apply(&db, writer.clone()).await.unwrap(),
            Command::Get(get) => {
                // TODO: remove
                // block command to let it propagate to replicas
                tokio::time::sleep(Duration::from_millis(20)).await;

                get.apply(&db, writer.clone()).await.unwrap();
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

                db.set(key, Bytes::from(val), expire);

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
                    if let Some(RESP::BulkString(cmd)) = arr.first() {
                        let cmd = String::from_utf8_lossy(cmd);
                        let resp = match cmd.to_lowercase().as_str() {
                            "config" => {
                                let conf_name = arr.get(2).unwrap();
                                match conf_name {
                                    RESP::BulkString(x) => {
                                        if x == "dir" {
                                            Some(RESP::Array(vec![
                                                RESP::BulkString(x.clone()),
                                                RESP::BulkString(Bytes::from(
                                                    dir.as_ref().clone().unwrap(),
                                                )),
                                            ]))
                                        } else if x == "dbfilename" {
                                            let db_filename = db_filename.clone();
                                            Some(RESP::Array(vec![
                                                RESP::BulkString(x.clone()),
                                                RESP::BulkString(Bytes::from(
                                                    db_filename.as_ref().clone().unwrap(),
                                                )),
                                            ]))
                                        } else {
                                            unreachable!();
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            }

                            "replconf" => {
                                let param = arr.get(1).unwrap();
                                if let RESP::BulkString(s) = param {
                                    // return from replicas
                                    if String::from_utf8_lossy(s).to_lowercase() == "ack" {
                                        let offset = arr.get(2).unwrap();
                                        if let RESP::BulkString(offset) = offset {
                                            let offset: usize =
                                                String::from_utf8_lossy(offset).parse().unwrap();
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
                                            if let RESP::BulkString(s) = x {
                                                String::from_utf8_lossy(s).parse::<usize>().ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap();
                                    let timeout = arr
                                        .get(2)
                                        .and_then(|x| {
                                            if let RESP::BulkString(s) = x {
                                                String::from_utf8_lossy(s).parse::<u64>().ok()
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
                                                RESP::BulkString(Bytes::from("REPLCONF")),
                                                RESP::BulkString(Bytes::from("GETACK")),
                                                RESP::BulkString(Bytes::from("*")),
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
                                    Some(RESP::BulkString(Bytes::from("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0")))
                                } else {
                                    Some(RESP::BulkString(Bytes::from("role:slave")))
                                }
                            }

                            _ => Some(RESP::Null),
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
    // let db = Arc::new(Mutex::new(db));

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

async fn init_db(dir: &Option<String>, db_filename: &Option<String>) -> Db {
    let db = Db::new();

    if let (Some(dir), Some(db_filename)) = (dir, db_filename) {
        let file = File::open(&Path::new(dir).join(db_filename)).await;

        match file {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                println!("{}/{} not found", dir, db_filename);
                db
            }
            Err(e) => panic!("failed to read rdb file: {}", &e),
            Ok(_) => {
                let file = file.unwrap();
                let reader = BufReader::new(file);

                let mut parser = Parser::new(reader);
                parser.parse().await.unwrap();

                parser.get_kv_pairs().for_each(|(k, (v, exp))| {
                    if let RESP::BulkString(x) = v {
                        match exp {
                            Some(exp) if *exp > SystemTime::now() => db.set(
                                k.clone(),
                                x.clone(),
                                exp.duration_since(SystemTime::now()).ok(),
                            ),
                            Some(_) => {} // expired
                            None => db.set(k.clone(), x.clone(), None),
                        }
                    }
                });
                db
            }
        }
    } else {
        db
    }
}

async fn handshake(
    listening_port: u16,
    replica_opt: Arc<Option<(Option<String>, Option<u16>)>>,
    db: Db,
) -> anyhow::Result<()> {
    if let Some((Some(host), Some(port))) = replica_opt.as_ref() {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let (reader, mut writer) = stream.into_split();
        let mut conn = Connection::new(reader);

        // The replica sends a PING to the master
        let resp = RESP::Array(vec![RESP::BulkString(Bytes::from("PING"))]);
        writer.write_all(resp.encode().as_bytes()).await?;

        // The replica sends REPLCONF twice to the master
        let resp1 = RESP::Array(vec![
            RESP::BulkString(Bytes::from("REPLCONF")),
            RESP::BulkString(Bytes::from("listening-port")),
            RESP::BulkString(Bytes::from(listening_port.to_string())),
        ]);

        let resp2 = RESP::Array(vec![
            RESP::BulkString(Bytes::from("REPLCONF")),
            RESP::BulkString(Bytes::from("capa")),
            RESP::BulkString(Bytes::from("psync2")),
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
            RESP::BulkString(Bytes::from("PSYNC")),
            RESP::BulkString(Bytes::from("?")),
            RESP::BulkString(Bytes::from("-1")),
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
                println!("replica receive command from master: {:?}", resp);
                if let RESP::Array(ref arr) = resp {
                    if let Some(RESP::BulkString(cmd)) = arr.first() {
                        let cmd = String::from_utf8_lossy(cmd);
                        match cmd.to_lowercase().as_str() {
                            "replconf" => {
                                let resp = RESP::Array(vec![
                                    RESP::BulkString(Bytes::from("REPLCONF")),
                                    RESP::BulkString(Bytes::from("ACK")),
                                    RESP::BulkString(Bytes::from(offset.to_string())),
                                ]);

                                let data = resp.encode();
                                println!("response: {:?}", data);
                                writer.write_all(data.as_bytes()).await.unwrap();
                            }
                            "ping" => {}
                            "set" => {
                                let key = arr.get(1).unwrap();
                                let val = arr.get(2).unwrap();

                                let px = arr.get(3);

                                if let RESP::BulkString(key) = key {
                                    if let RESP::BulkString(val) = val {
                                        let mut exp = None;
                                        if px.is_some_and(|x| {
                                            *x == RESP::BulkString(Bytes::from("px"))
                                        }) {
                                            let exp_data = arr.get(4).unwrap();
                                            if let RESP::BulkString(e) = exp_data {
                                                let u64_exp: u64 =
                                                    String::from_utf8_lossy(e).parse().unwrap();
                                                exp = Some(Duration::from_millis(u64_exp));
                                            }
                                        }
                                        db.set(
                                            String::from_utf8_lossy(key).to_string(),
                                            val.clone(),
                                            exp,
                                        );
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
