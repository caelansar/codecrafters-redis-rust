use crate::{
    parse::{Parse, ParseError},
    protocol::RESP,
};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::MutexGuard};

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

    pub(crate) async fn apply(self, mut dst: MutexGuard<'_, OwnedWriteHalf>) -> anyhow::Result<()> {
        let resp = RESP::SimpleString(self.message.map_or("PONG".into(), |x| x.clone()));

        dst.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
