use crate::parse::{Parse, ParseError};

/// Returns PONG if no argument is provided, otherwise return a copy
/// of the argument as a bulk.
#[derive(Debug, PartialOrd, PartialEq)]
pub struct Ping {
    message: Option<String>,
}

impl Ping {
    pub fn new() -> Ping {
        Ping { message: None }
    }

    pub fn message(&self) -> Option<&String> {
        self.message.as_ref()
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Ping> {
        let mut message = None;
        match parse.next_string() {
            Ok(s) => {
                message = Some(s);
            }
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `PING` options.
            Err(ParseError::EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }

        Ok(Ping { message })
    }
}
