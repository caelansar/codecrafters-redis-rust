use crate::parse::Parse;
use bytes::Bytes;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Echo {
    message: Bytes,
}

impl Echo {
    pub fn new(message: Bytes) -> Echo {
        Echo { message }
    }

    pub fn message(&self) -> Bytes {
        self.message.clone()
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Echo> {
        let message = parse.next_bytes()?;

        Ok(Echo { message })
    }
}
