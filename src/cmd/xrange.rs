use crate::parse::Parse;

/// The XRANGE command retrieves a range of entries from a stream.
/// It takes two arguments: start and end. Both are entry IDs. The
/// command returns all entries in the stream with IDs between the
/// start and end IDs. This range is "inclusive", which means that
/// the response will includes entries with IDs that are equal to
/// the start and end IDs.
#[derive(Debug, PartialEq)]
pub struct XRange {
    stream_key: String,
    start: Time,
    end: String,
}

#[derive(Debug, PartialEq)]
pub enum Time {
    Wildcard,
    Partially(String),
    Specified(String),
}

impl XRange {
    pub fn new(stream_key: impl ToString, start: Time, end: String) -> Self {
        Self {
            stream_key: stream_key.to_string(),
            start,
            end,
        }
    }

    pub fn key(&self) -> &str {
        &self.stream_key
    }

    pub fn range(&self) -> (String, String) {
        let start = match &self.start {
            Time::Specified(s) => s.clone(),
            Time::Partially(s) => {
                let mut s = s.clone();
                s.push_str("-0");
                s
            }
            Time::Wildcard => "0-0".to_string(),
        };
        (start, self.end.clone())
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Self> {
        let stream_key = parse.next_string()?;

        let start_str = parse.next_string()?;
        let start = match start_str.as_str() {
            "-" => Time::Wildcard,
            x if !x.contains("-") => Time::Partially(x.to_string()),
            s => Time::Specified(s.to_string()),
        };

        Ok(Self::new(stream_key, start, parse.next_string()?))
    }
}
