use self::echo::Echo;
use self::get::Get;
use self::keys::Keys;
use self::ping::Ping;
use self::publish::Publish;
use self::r#type::Type;
use self::set::Set;
use self::subscribe::Subscribe;
use self::xadd::Xadd;
use self::xrange::XRange;
use self::xread::XRead;
use crate::cmd::unsubscribe::Unsubscribe;
use crate::parse::Parse;
use crate::protocol::RESP;

mod echo;
mod get;
mod keys;
mod ping;
mod publish;
mod set;
mod subscribe;
pub mod time_spec;
mod r#type;
mod unsubscribe;
mod xadd;
mod xrange;
mod xread;

#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Echo(Echo),
    Keys(Keys),
    Ping(Ping),
    Type(Type),
    Xadd(Xadd),
    XRange(XRange),
    XRead(XRead),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Raw(RESP),
}

impl Command {
    pub fn from_resp_frame(resp: RESP) -> anyhow::Result<Command> {
        let mut parse = Parse::new(resp.clone())?;

        let name = parse.next_string()?.to_lowercase();

        let command = match name.as_str() {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "echo" => Command::Echo(Echo::parse_frames(&mut parse)?),
            "keys" => Command::Keys(Keys::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "type" => Command::Type(Type::parse_frames(&mut parse)?),
            "xadd" => Command::Xadd(Xadd::parse_frames(&mut parse)?),
            "xrange" => Command::XRange(XRange::parse_frames(&mut parse)?),
            "xread" => Command::XRead(XRead::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            _ => Command::Raw(resp),
        };

        if !matches!(command, Command::Raw(_)) {
            parse.finish()?;
        }

        Ok(command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::time_spec::TimeSepc;
    use crate::cmd::xrange::XRange;
    use bytes::Bytes;
    use std::time::Duration;

    struct Testcase {
        resp: RESP,
        cmd: Command,
    }

    #[test]
    fn test_parse() {
        let testcases = vec![
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("GET")),
                    RESP::BulkString(Bytes::from("hey")),
                ]),
                cmd: Command::Get(Get::new("hey")),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("SET")),
                    RESP::BulkString(Bytes::from("key")),
                    RESP::BulkString(Bytes::from("val")),
                    RESP::BulkString(Bytes::from("PX")),
                    RESP::BulkString(Bytes::from("100")),
                ]),
                cmd: Command::Set(Set::new(
                    "key",
                    "val".into(),
                    Some(Duration::from_millis(100)),
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("SET")),
                    RESP::BulkString(Bytes::from("key")),
                    RESP::BulkString(Bytes::from("val")),
                    RESP::BulkString(Bytes::from("EX")),
                    RESP::BulkString(Bytes::from("100")),
                ]),
                cmd: Command::Set(Set::new(
                    "key",
                    "val".into(),
                    Some(Duration::from_secs(100)),
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("XADD")),
                    RESP::BulkString(Bytes::from("stream_key")),
                    RESP::BulkString(Bytes::from("1-0")),
                    RESP::BulkString(Bytes::from("k")),
                    RESP::BulkString(Bytes::from("1")),
                ]),
                cmd: Command::Xadd(Xadd::new(
                    "stream_key",
                    "1-0",
                    vec![("k".to_string(), "1".to_string())],
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("XRANGE")),
                    RESP::BulkString(Bytes::from("stream_key")),
                    RESP::BulkString(Bytes::from("1-0")),
                    RESP::BulkString(Bytes::from("+")),
                ]),
                cmd: Command::XRange(XRange::new(
                    "stream_key",
                    TimeSepc::Specified(1, 0),
                    TimeSepc::EndWildcard,
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("XREAD")),
                    RESP::BulkString(Bytes::from("streams")),
                    RESP::BulkString(Bytes::from("stream_key")),
                    RESP::BulkString(Bytes::from("1-0")),
                ]),
                cmd: Command::XRead(XRead::new(
                    None,
                    vec!["stream_key".into()],
                    vec![TimeSepc::Specified(1, 0)],
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("XREAD")),
                    RESP::BulkString(Bytes::from("block")),
                    RESP::BulkString(Bytes::from("1000")),
                    RESP::BulkString(Bytes::from("streams")),
                    RESP::BulkString(Bytes::from("stream_key")),
                    RESP::BulkString(Bytes::from("1-0")),
                ]),
                cmd: Command::XRead(XRead::new(
                    Some(Duration::from_millis(1000)),
                    vec!["stream_key".into()],
                    vec![TimeSepc::Specified(1, 0)],
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("XREAD")),
                    RESP::BulkString(Bytes::from("block")),
                    RESP::BulkString(Bytes::from("1000")),
                    RESP::BulkString(Bytes::from("streams")),
                    RESP::BulkString(Bytes::from("stream_key")),
                    RESP::BulkString(Bytes::from("$")),
                ]),
                cmd: Command::XRead(XRead::new(
                    Some(Duration::from_millis(1000)),
                    vec!["stream_key".into()],
                    vec![TimeSepc::EndWildcard],
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("XREAD")),
                    RESP::BulkString(Bytes::from("streams")),
                    RESP::BulkString(Bytes::from("stream_key")),
                    RESP::BulkString(Bytes::from("stream_key_1")),
                    RESP::BulkString(Bytes::from("1-0")),
                    RESP::BulkString(Bytes::from("2-0")),
                ]),
                cmd: Command::XRead(XRead::new(
                    None,
                    vec!["stream_key".into(), "stream_key_1".into()],
                    vec![TimeSepc::Specified(1, 0), TimeSepc::Specified(2, 0)],
                )),
            },
            Testcase {
                resp: RESP::Array(vec![
                    RESP::BulkString(Bytes::from("PUBLISH")),
                    RESP::BulkString(Bytes::from("channel")),
                    RESP::BulkString(Bytes::from("hello")),
                ]),
                cmd: Command::Publish(Publish::new("channel", Bytes::from("hello"))),
            },
        ];

        testcases.into_iter().for_each(|testcase| {
            let cmd = Command::from_resp_frame(testcase.resp).unwrap();

            assert_eq!(cmd, testcase.cmd);

            if let Command::Set(set) = cmd {
                println!("{:?}", set.into_frame());
            }
        });
    }
}
