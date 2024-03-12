use std::collections::BTreeMap;

use crate::parse::Parse;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Xadd {
    stream_key: String,
    stream_id: String,
    data: Vec<(String, String)>,
}

impl Xadd {
    pub fn new(key: impl ToString, id: impl ToString, data: Vec<(String, String)>) -> Self {
        Self {
            stream_key: key.to_string(),
            stream_id: id.to_string(),
            data,
        }
    }

    pub fn key(&self) -> &str {
        &self.stream_key
    }

    pub fn data(&self) -> Vec<(String, String)> {
        self.data.clone()
    }

    pub fn id(&self) -> &str {
        &self.stream_id
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
}
