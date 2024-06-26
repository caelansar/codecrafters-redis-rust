use crate::{parse::Parse, protocol::RESP};
use bytes::Bytes;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::MutexGuard};

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Echo {
    message: Bytes,
}

impl Echo {
    pub fn new(message: Bytes) -> Echo {
        Echo { message }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Echo> {
        let message = parse.next_bytes()?;

        Ok(Echo { message })
    }

    pub(crate) async fn apply(
        &self,
        mut dst: MutexGuard<'_, OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        let resp = RESP::BulkString(self.message.clone());

        dst.write_all(resp.encode().as_bytes()).await.unwrap();

        Ok(())
    }
}
