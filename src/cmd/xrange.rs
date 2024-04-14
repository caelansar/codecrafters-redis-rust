use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::MutexGuard;

use crate::cmd::time_spec::TimeSepc;
use crate::parse::Parse;
use crate::protocol::RESP;
use crate::storage::Db;
use std::ops;

/// The XRANGE command retrieves a range of entries from a stream.
/// It takes two arguments: start and end. Both are entry IDs. The
/// command returns all entries in the stream with IDs between the
/// start and end IDs. This range is "inclusive", which means that
/// the response will include entries with IDs that are equal to
/// the start and end IDs.
#[derive(Debug, PartialEq)]
pub struct XRange {
    stream_key: String,
    start: TimeSepc,
    end: TimeSepc,
}

impl XRange {
    pub fn new(stream_key: impl ToString, start: TimeSepc, end: TimeSepc) -> Self {
        Self {
            stream_key: stream_key.to_string(),
            start,
            end,
        }
    }

    fn range(&self) -> impl ops::RangeBounds<TimeSepc> {
        self.start.clone()..=self.end.clone()
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        let stream_key = parse.next_string()?;

        let start_str = parse.next_string()?;
        let end_str = parse.next_string()?;

        let start = start_str.parse()?;
        let end = end_str.parse()?;

        Ok(Self::new(stream_key, start, end))
    }

    pub(crate) async fn apply(
        &self,
        db: &Db,
        mut dst: MutexGuard<'_, OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        use std::ops::RangeBounds;

        let mut arr = Vec::new();
        let resp = match db.get_stream(&self.stream_key) {
            Some(stream) => {
                let iter = stream.into_iter().filter(|(id, _)| {
                    let t = id.parse().unwrap();
                    self.range().contains(&t)
                });
                iter.for_each(|(id, data)| {
                    let items = vec![RESP::BulkString(Bytes::from(id)), data.into()];
                    arr.push(RESP::Array(items));
                });

                RESP::Array(arr)
            }
            None => RESP::Array(arr),
        };

        dst.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
