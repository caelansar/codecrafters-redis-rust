use crate::{parse::Parse, protocol::RESP, storage::Db};
use bytes::Bytes;
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex};

/// Posts a message to the given channel.
///
/// Send a message into a channel without any knowledge of individual consumers.
/// Consumers may subscribe to channels in order to receive the messages.
///
/// Channel names have no relation to the key-value namespace. Publishing on a
/// channel named "foo" has no relation to setting the "foo" key.
#[derive(Debug, PartialEq)]
pub struct Publish {
    /// Name of the channel on which the message should be published.
    channel: String,

    /// The message to publish.
    message: Bytes,
}

impl Publish {
    /// Create a new `Publish` command which sends `message` on `channel`.
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// Parse a `Publish` instance from a received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Publish> {
        // The `PUBLISH` string has already been consumed. Extract the `channel`
        // and `message` values from the frame.
        //
        // The `channel` must be a valid string.
        let channel = parse.next_string()?;

        // The `message` is arbitrary bytes.
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: Arc<Mutex<OwnedWriteHalf>>,
    ) -> anyhow::Result<()> {
        let num_subscribers = db.publish(&self.channel, self.message);

        // The number of subscribers is returned as the response to the publish
        // request.
        let resp = RESP::Integer(num_subscribers as i64);

        // Write the frame to the client.
        dst.lock().await.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
