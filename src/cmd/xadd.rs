use crate::parse::Parse;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Xadd {
    stream_key: String,
    stream_id: String,
}

impl Xadd {
    pub fn new(key: impl ToString, id: impl ToString) -> Self {
        Self {
            stream_key: key.to_string(),
            stream_id: id.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.stream_key
    }

    pub fn id(&self) -> &str {
        &self.stream_id
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        let stream_key = parse.next_string()?;

        // TODO: XADD supports auto-generating stream IDs
        let stream_id = parse.next_string()?;

        Ok(Self {
            stream_key,
            stream_id,
        })
    }
}
