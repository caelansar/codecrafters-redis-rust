use crate::cmd::time_spec::TimeSepc;
use crate::parse::Parse;
use anyhow::bail;

/// XREAD is used to read data from one or more streams, starting from a specified entry ID.
#[derive(Debug, PartialEq)]
pub struct XRead {
    stream_key: String,
    start: TimeSepc,
}

impl XRead {
    pub fn new(stream_key: impl ToString, start: TimeSepc) -> Self {
        Self {
            stream_key: stream_key.to_string(),
            start,
        }
    }

    pub fn key_and_start(&self) -> (&str, &TimeSepc) {
        (&self.stream_key, &self.start)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        if parse.next_string()? != "streams" {
            bail!("expect to be streams")
        }

        let stream_key = parse.next_string()?;
        let start_str = parse.next_string()?;

        let start = start_str.parse()?;

        Ok(Self::new(stream_key, start))
    }
}
