use crate::cmd::Command;
use crate::connection::Connection;
use crate::parse;
use crate::parse::Parse;
use crate::protocol::RESP;
use crate::storage::Db;
use bytes::Bytes;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::select;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::{Stream, StreamExt, StreamMap};

/// Subscribes the client to one or more channels.
///
/// Once the client enters the subscribed state, it is not supposed to issue any
/// other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING and QUIT commands.
#[derive(Debug, PartialEq)]
pub struct Subscribe {
    pub channels: Vec<String>,
}
/// Stream of messages. The stream receives messages from the
/// `broadcast::Receiver`. We use `stream!` to create a `Stream` that consumes
/// messages. Because `stream!` values cannot be named, we box the stream using
/// a trait object.
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// Creates a new `Subscribe` command to listen on the specified channels.
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// Parse a `Subscribe` instance from a received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Subscribe> {
        use parse::ParseError::EndOfStream;

        // The `SUBSCRIBE` string has already been consumed. At this point,
        // there is one or more strings remaining in `parse`. These represent
        // the channels to subscribe to.
        //
        // Extract the first string. If there is none, the the frame is
        // malformed and the error is bubbled up.
        let mut channels = vec![parse.next_string()?];

        // Now, the remainder of the frame is consumed. Each value must be a
        // string or the frame is malformed. Once all values in the frame have
        // been consumed, the command is fully parsed.
        loop {
            match parse.next_string() {
                // A string has been consumed from the `parse`, push it into the
                // list of channels to subscribe to.
                Ok(s) => channels.push(s),
                // The `EndOfStream` error indicates there is no further data to
                // parse.
                Err(EndOfStream) => break,
                // All other errors are bubbled up, resulting in the connection
                // being terminated.
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// Apply the `Subscribe` command to the specified `Db` instance.
    ///
    /// This function is the entry point and includes the initial list of
    /// channels to subscribe to. Additional `subscribe` and `unsubscribe`
    /// commands may be received from the client and the list of subscriptions
    /// are updated accordingly.
    ///
    /// [here]: https://redis.io/topics/pubsub
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        conn: &mut Connection<OwnedReadHalf>,
        dst: Arc<Mutex<OwnedWriteHalf>>,
    ) -> anyhow::Result<()> {
        // Each individual channel subscription is handled using a
        // `sync::broadcast` channel. Messages are then fanned out to all
        // clients currently subscribed to the channels.
        //
        // An individual client may subscribe to multiple channels and may
        // dynamically add and remove channels from its subscription set. To
        // handle this, a `StreamMap` is used to track active subscriptions. The
        // `StreamMap` merges messages from individual broadcast channels as
        // they are received.
        let mut subscriptions = StreamMap::new();

        println!("channel: {:?}", self.channels);

        loop {
            // `self.channels` is used to track additional channels to subscribe
            // to. When new `SUBSCRIBE` commands are received during the
            // execution of `apply`, the new channels are pushed onto this vec.
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst.clone()).await?;
            }

            // Wait for one of the following to happen:
            //
            // - Receive a message from one of the subscribed channels.
            // - Receive a subscribe or unsubscribe command from the client.
            // - A server shutdown signal.
            select! {
                // Receive messages from subscribed channels
                Some((channel_name, msg)) = subscriptions.next() => {
                    let resp = make_message_frame(channel_name, msg);

                    dst
                    .lock()
                    .await
                    .write_all(resp.encode().as_bytes())
                    .await
                    .unwrap();
                }
                res = conn.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // This happens if the remote client has disconnected.
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst.clone(),
                    ).await?;
                }
            };
        }
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: Arc<Mutex<OwnedWriteHalf>>,
) -> anyhow::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // Subscribe to the channel.
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // If we lagged in consuming messages, just resume.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // Track subscription in this client's subscription set.
    subscriptions.insert(channel_name.clone(), rx);

    // Respond with the successful subscription
    let resp = make_subscribe_frame(channel_name, subscriptions.len());
    dst.lock()
        .await
        .write_all(resp.encode().as_bytes())
        .await
        .unwrap();

    Ok(())
}

/// Handle a command received while inside `Subscribe::apply`. Only subscribe
/// and unsubscribe commands are permitted in this context.
///
/// Any new subscriptions are appended to `subscribe_to` instead of modifying
/// `subscriptions`.
async fn handle_command(
    frame: RESP,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: Arc<Mutex<OwnedWriteHalf>>,
) -> anyhow::Result<()> {
    // A command has been received from the client.
    //
    // Only `SUBSCRIBE` and `UNSUBSCRIBE` commands are permitted
    // in this context.
    match Command::from_resp_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // The `apply` method will subscribe to the channels we add to this
            // vector.
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // If no channels are specified, this requests unsubscribing from
            // **all** channels. To implement this, the `unsubscribe.channels`
            // vec is populated with the list of channels currently subscribed
            // to.
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let resp = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.lock()
                    .await
                    .write_all(resp.encode().as_bytes())
                    .await
                    .unwrap();
            }
        }
        command => {
            unreachable!()
        }
    }
    Ok(())
}

/// Creates the response to a subcribe request.
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> RESP {
    RESP::Array(vec![
        RESP::BulkString(Bytes::from_static(b"subscribe")),
        RESP::BulkString(Bytes::from(channel_name)),
        RESP::Integer(num_subs as i64),
    ])
}

/// Creates the response to an unsubcribe request.
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> RESP {
    RESP::Array(vec![
        RESP::BulkString(Bytes::from_static(b"unsubscribe")),
        RESP::BulkString(Bytes::from(channel_name)),
        RESP::Integer(num_subs as i64),
    ])
}

/// Creates a message informing the client about a new message on a channel that
/// the client subscribes to.
fn make_message_frame(channel_name: String, msg: Bytes) -> RESP {
    RESP::Array(vec![
        RESP::BulkString(Bytes::from_static(b"message")),
        RESP::BulkString(Bytes::from(channel_name)),
        RESP::BulkString(msg),
    ])
}
