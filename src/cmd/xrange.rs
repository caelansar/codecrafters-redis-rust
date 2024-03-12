use crate::parse::{Parse, ParseError};

/// The XRANGE command retrieves a range of entries from a stream.
/// It takes two arguments: start and end. Both are entry IDs. The
/// command returns all entries in the stream with IDs between the
/// start and end IDs. This range is "inclusive", which means that
/// the response will includes entries with IDs that are equal to
/// the start and end IDs.
#[derive(Debug, PartialOrd, PartialEq)]
pub struct XRange {
    stream_key: String,
    start: String,
    end: String,
}

impl XRange {
    pub fn new(stream_key: impl ToString, start: String, end: String) -> Self {
        Self {
            stream_key: stream_key.to_string(),
            start,
            end,
        }
    }

    pub fn key(&self) -> &str {
        &self.stream_key
    }

    pub fn range(&self) -> (&str, &str) {
        (&self.start, &self.end)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        let stream_key = parse.next_string()?;

        Ok(Self::new(
            stream_key,
            parse.next_string()?,
            parse.next_string()?,
        ))
    }
}
