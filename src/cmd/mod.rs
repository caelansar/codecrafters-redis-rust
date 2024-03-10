mod echo;
mod get;
mod keys;
mod ping;
mod set;
mod r#type;

use crate::cmd::echo::Echo;
use crate::cmd::get::Get;
use crate::cmd::keys::Keys;
use crate::cmd::ping::Ping;
use crate::cmd::r#type::Type;
use crate::cmd::set::Set;
use crate::parse::Parse;
use crate::protocol::RESP;

#[derive(Debug, PartialOrd, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Echo(Echo),
    Keys(Keys),
    Ping(Ping),
    Type(Type),
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
