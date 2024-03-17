use crate::cmd::time_spec::TimeSepc;
use crate::parse::Parse;
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

    pub fn key(&self) -> &str {
        &self.stream_key
    }

    pub fn range(&self) -> impl ops::RangeBounds<TimeSepc> {
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
}
