use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::MutexGuard};

use crate::{parse::Parse, protocol::RESP, storage::Db};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug, PartialOrd, PartialEq)]
pub struct Get {
    /// Name of the key to get
    key: String,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Get> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    pub(crate) async fn apply(
        &self,
        db: &Db,
        mut dst: MutexGuard<'_, OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        let resp = match db.get(&self.key) {
            Some(e) => RESP::BulkString(e),
            None => RESP::Null,
        };
        dst.write_all(resp.encode().as_bytes()).await?;

        Ok(())
    }
}
