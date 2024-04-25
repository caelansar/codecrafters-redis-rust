use crate::parse::Parse;
use crate::protocol::RESP;
use crate::storage::Db;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::MutexGuard;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Xadd {
    stream_key: String,
    stream_id: String,
    data: StreamData,
}

pub type StreamData = Vec<(String, String)>;

impl From<StreamData> for RESP {
    fn from(value: StreamData) -> Self {
        let mut arr = Vec::new();
        value.into_iter().for_each(|(k, v)| {
            arr.push(RESP::BulkString(Bytes::from(k)));
            arr.push(RESP::BulkString(Bytes::from(v)));
        });
        RESP::Array(arr)
    }
}

impl Xadd {
    pub fn new(key: impl ToString, id: impl ToString, data: StreamData) -> Self {
        Self {
            stream_key: key.to_string(),
            stream_id: id.to_string(),
            data,
        }
    }

    fn data(&self) -> StreamData {
        self.data.clone()
    }

    fn id(&self) -> &str {
        &self.stream_id
    }

    fn to_item(&self) -> Vec<RESP> {
        vec![
            RESP::BulkString(Bytes::from(self.id().to_string())),
            self.data().into(),
        ]
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        let stream_key = parse.next_string()?;

        let stream_id = parse.next_string()?;

        let mut data = Vec::new();
        while let Ok(d) = parse.next_string() {
            data.push((d, parse.next_string()?));
        }

        Ok(Self {
            stream_key,
            stream_id,
            data,
        })
    }

    pub(crate) async fn apply(
        self,
        db: &Db,
        mut dst: MutexGuard<'_, OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        let mut id = self.stream_id.clone();

        match id.as_str() {
            "*" => {
                let now = SystemTime::now();
                let duration = now.duration_since(UNIX_EPOCH)?;

                id = format!("{}-{}", duration.as_millis(), 0);
            }
            _ => {
                let parts = id.split_once('-');
                if let Some((time, seq)) = parts {
                    if seq == "*" {
                        match db.get_stream(&self.stream_key) {
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

        let resp = match db.set_stream(&self.stream_key, &id, self.data()) {
            Ok(_) => RESP::BulkString(Bytes::from(id)),
            Err(e) => RESP::Error(e.to_string()),
        };

        let senders = db.get_all_block(&self.stream_key);
        senders
            .into_iter()
            .for_each(|sender| sender.send(RESP::Array(self.to_item())).unwrap());

        dst.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
