use std::sync::Arc;

use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex};

use crate::{parse::Parse, protocol::RESP, storage::Db};

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn new(key: impl ToString) -> Type {
        Type {
            key: key.to_string(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Type> {
        let key = parse.next_string()?;

        Ok(Type { key })
    }

    pub(crate) async fn apply(
        &self,
        db: &Db,
        dst: Arc<Mutex<OwnedWriteHalf>>,
    ) -> anyhow::Result<()> {
        let resp = match db.get(&self.key) {
            Some(_) => RESP::SimpleString("string".to_string()),
            None => {
                if db.get_stream(&self.key).is_some() {
                    RESP::SimpleString("stream".to_string())
                } else {
                    RESP::SimpleString("none".to_string())
                }
            }
        };
        dst.lock().await.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
