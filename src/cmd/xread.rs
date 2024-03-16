use crate::cmd::time_spec::TimeSepc;
use crate::parse::Parse;
use anyhow::bail;

/// XREAD is used to read data from one or more streams, starting from a specified entry ID.
#[derive(Debug, PartialEq)]
pub struct XRead {
    stream_keys: Vec<String>,
    starts: Vec<TimeSepc>,
}

impl XRead {
    pub fn new(stream_keys: Vec<String>, starts: Vec<TimeSepc>) -> Self {
        Self {
            stream_keys,
            starts,
        }
    }

    pub fn key_start_pairs(&self) -> Vec<(&str, &TimeSepc)> {
        self.stream_keys
            .iter()
            .zip(self.starts.iter())
            .map(|(key, start)| (key.as_str(), start))
            .collect()
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        if parse.next_string()? != "streams" {
            bail!("expect to be streams")
        }

        let mut params = Vec::new();
        while let Ok(d) = parse.next_string() {
            params.push(d);
        }

        let mut stream_keys = Vec::new();
        let mut starts = Vec::new();

        let mid = params.len() / 2;

        params.iter().take(mid).for_each(|key| {
            stream_keys.push(key.to_string());
        });
        params
            .iter()
            .skip(mid)
            .for_each(|start| starts.push(start.parse().unwrap()));

        Ok(Self::new(stream_keys, starts))
    }
}
