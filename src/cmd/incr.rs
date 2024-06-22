use crate::{parse::Parse, protocol::RESP, storage::Db};
use bytes::Bytes;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::MutexGuard};

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Incr {
    key: String,
}

impl Incr {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        Ok(Incr { key })
    }

    pub(crate) async fn apply(
        &self,
        db: &Db,
        mut dst: MutexGuard<'_, OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        let resp = db.get(&self.key);

        let res = match resp {
            Some(x) => {
                let s = String::from_utf8_lossy(&x);
                match s.parse::<i64>() {
                    Ok(i) => {
                        db.set(self.key.clone(), Bytes::from((i + 1).to_string()), None);
                        RESP::Integer(i + 1)
                    }
                    Err(_) => {
                        RESP::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }
            None => {
                db.set(self.key.clone(), Bytes::from(1.to_string()), None);
                RESP::Integer(1)
            }
        };

        dst.write_all(res.encode().as_bytes()).await?;

        Ok(())
    }
}
