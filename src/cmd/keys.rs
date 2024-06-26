use crate::{parse::Parse, protocol::RESP, storage::Db};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::MutexGuard};

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Keys {
    pattern: String,
}

#[allow(unused)]
impl Keys {
    pub fn new(pattern: impl ToString) -> Keys {
        Keys {
            pattern: pattern.to_string(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Keys> {
        let message = parse.next_string()?;

        Ok(Keys { pattern: message })
    }

    pub(crate) async fn apply(
        &self,
        db: &Db,
        mut dst: MutexGuard<'_, OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        // TODO: get keys by pattern
        let pattern = &self.pattern;
        assert_eq!("*", pattern);

        let resp = RESP::Array(db.keys());

        dst.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
