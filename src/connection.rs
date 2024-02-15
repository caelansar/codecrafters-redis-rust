use crate::protocol::{Decoder, Error, RESP};
use anyhow::anyhow;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug)]
pub struct Connection<R: AsyncRead + Unpin> {
    // Underlying reader
    stream: R,
    // The buffer for reading RESP frames
    buffer: BytesMut,
}

impl<R: AsyncRead + Unpin> Connection<R> {
    /// Create a new `Connection`
    pub fn new(read: R) -> Connection<R> {
        Connection {
            stream: read,
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn skip_rdb(&mut self) -> anyhow::Result<usize> {
        let data = &self.buffer[..];
        println!(
            "existed data: {:?}, length: {}",
            String::from_utf8_lossy(data),
            data.len()
        );

        let mut len = data.len();
        if len > 93 {
            self.buffer.advance(93);
        }

        while len < 93 {
            let mut empty_rdb = Vec::with_capacity(93 - len);
            len += self.stream.read_buf(&mut empty_rdb).await?;
        }
        Ok(len)
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    pub async fn read_frame(&mut self) -> anyhow::Result<Option<RESP>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow!("connection reset by peer"));
                }
            }
        }
    }

    /// Parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> anyhow::Result<Option<RESP>> {
        let data = &self.buffer[..];
        let cmd = String::from_utf8_lossy(data);

        let mut decoder = Decoder::new(cmd.as_ref());
        let resp = decoder.parse();

        match resp {
            Ok(resp) => {
                // Discard the parsed data from the read buffer
                self.buffer.advance(decoder.position());

                Ok(resp)
            }
            Err(Error::Incomplete) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_hex;
    use crate::rdb::consts;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_connection() {
        let data = "+OK\r\n+PING\r\n";
        let reader = BufReader::new(data.as_bytes());

        let mut conn = Connection::new(reader);
        let resp = conn.read_frame().await.unwrap();
        assert_eq!(Some(RESP::SimpleString("OK".to_string())), resp);

        let resp = conn.read_frame().await.unwrap();
        assert_eq!(Some(RESP::SimpleString("PING".to_string())), resp);
    }

    #[tokio::test]
    async fn decode_empty_rdb() {
        let binary_rdb = decode_hex(consts::EMPTY_RDB).unwrap();
        let mut data = format!("${}\r\n", binary_rdb.len()).into_bytes();
        data.extend(binary_rdb);
        assert_eq!(93, data.len())
    }
}
