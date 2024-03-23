use crate::parse::Parse;
use crate::protocol::RESP;
use bytes::Bytes;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Xadd {
    stream_key: String,
    stream_id: String,
    data: StreamData,
}

pub type StreamData = Vec<(String, String)>;

impl From<StreamData> for RESP {
    fn from(value: StreamData) -> Self {
        let mut arr = Vec::new();
        value.into_iter().for_each(|(k, v)| {
            arr.push(RESP::BulkString(Bytes::from(k)));
            arr.push(RESP::BulkString(Bytes::from(v)));
        });
        RESP::Array(arr)
    }
}

impl Xadd {
    pub fn new(key: impl ToString, id: impl ToString, data: StreamData) -> Self {
        Self {
            stream_key: key.to_string(),
            stream_id: id.to_string(),
            data,
        }
    }

    pub fn key(&self) -> &str {
        &self.stream_key
    }

    pub fn data(&self) -> StreamData {
        self.data.clone()
    }

    pub fn id(&self) -> &str {
        &self.stream_id
    }

    pub fn to_item(&self) -> Vec<RESP> {
        vec![
            RESP::BulkString(Bytes::from(self.id().to_string())),
            self.data().into(),
        ]
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
