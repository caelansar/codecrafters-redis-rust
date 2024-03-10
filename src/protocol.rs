use bytes::Bytes;
use std::str::FromStr;

// Redis Serialization Protocol
#[derive(Debug, PartialOrd, PartialEq, Clone)]
pub enum RESP {
    // Simple strings are encoded as a plus (+) character, followed by a string.
    // The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
    SimpleString(String),
    // A bulk string represents a single binary string. The string can be of any size,
    // but by default, Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
    BulkString(Bytes),
    // Clients send commands to the Redis server as RESP arrays. Similarly, some Redis commands that
    // return collections of elements use arrays as their replies. An example is the LRANGE command that
    // returns elements of a list.
    Array(Vec<RESP>),
    // A CRLF-terminated string that represents a signed, base-10, 64-bit integer.
    Integer(i64),
    // The null data type represents non-existent values.
    Null,
    // RESP has specific data types for errors. Simple errors, or simply just errors, are similar to simple
    // strings, but their first character is the minus (-) character.
    Error(String),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("Incomplete resp frame")]
    Incomplete,
    #[error("Invalid frame format")]
    InvalidFormat,
    #[error("{0}")]
    Other(String),
}

impl FromStr for RESP {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parser = Decoder::new(s);
        parser.parse().map(|x| x.unwrap())
    }
}

impl From<RESP> for String {
    fn from(value: RESP) -> Self {
        value.encode()
    }
}

impl RESP {
    pub fn encode(&self) -> String {
        let mut res = String::new();
        match self {
            Self::SimpleString(s) => {
                res.push_str(&format!("+{}\r\n", s));
            }
            Self::Integer(i) => {
                res.push_str(&format!(":{}\r\n", i));
            }
            Self::BulkString(s) => {
                res.push_str(&format!(
                    "${}\r\n{}\r\n",
                    s.len(),
                    String::from_utf8_lossy(s)
                ));
            }
            Self::Array(v) => {
                res.push_str(&format!("*{}\r\n", v.len()));

                v.iter().for_each(|r| {
                    res.push_str(r.encode().as_str());
                })
            }
            Self::Null => {
                res.push_str("$-1\r\n");
            }
            Self::Error(e) => {
                res.push_str(&format!("-{}\r\n", e));
            }
        }
        res
    }
}

pub(crate) struct Decoder<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(input: &'a str) -> Decoder<'a> {
        Decoder { input, pos: 0 }
    }

    pub(crate) fn position(&self) -> usize {
        self.pos
    }

    pub(crate) fn parse(&mut self) -> Result<Option<RESP>, Error> {
        if self.pos >= self.input.len() {
            return Ok(None);
        }

        let cmd = &self.input[self.pos..];

        match cmd.chars().next() {
            Some('*') => {
                self.pos += 1;
                self.parse_array()
            }
            Some('$') => {
                self.pos += 1;
                self.parse_bulk_string()
            }
            Some('+') => {
                self.pos += 1;
                self.parse_simple_string()
            }
            Some(':') => {
                self.pos += 1;
                self.parse_int()
            }
            _ => {
                println!("invalid cmd: <{:?}>", cmd);
                unreachable!()
            }
        }
    }
    fn parse_array(&mut self) -> Result<Option<RESP>, Error> {
        if self.pos >= self.input.len() {
            return Err(Error::Incomplete);
        }

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();
        let item_count: usize = res.parse().map_err(|_| Error::InvalidFormat)?;

        self.pos += res.len();
        self.pos += 2;

        let mut arr = Vec::with_capacity(item_count);

        (0..item_count).try_for_each(|_| {
            let item = self.parse()?;
            if let Some(item) = item {
                arr.push(item);
            }
            Ok(())
        })?;

        Ok(Some(RESP::Array(arr)))
    }

    fn parse_int(&mut self) -> Result<Option<RESP>, Error> {
        if self.pos >= self.input.len() {
            return Err(Error::Incomplete);
        }

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();

        self.pos += res.len();

        if !&self.input[self.pos..].starts_with("\r\n") {
            return Err(Error::Incomplete);
        }
        self.pos += 2;

        Ok(Some(RESP::Integer(
            res.parse().map_err(|_| Error::InvalidFormat)?,
        )))
    }

    fn parse_simple_string(&mut self) -> Result<Option<RESP>, Error> {
        if self.pos >= self.input.len() {
            return Err(Error::Incomplete);
        }

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();

        self.pos += res.len();

        if !&self.input[self.pos..].starts_with("\r\n") {
            return Err(Error::Incomplete);
        }
        self.pos += 2;

        Ok(Some(RESP::SimpleString(res)))
    }

    fn parse_bulk_string(&mut self) -> Result<Option<RESP>, Error> {
        if self.pos >= self.input.len() {
            return Err(Error::Incomplete);
        }
        let mut s = String::new();

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();
        let len: usize = res.parse().map_err(|_| Error::InvalidFormat)?;

        self.pos += res.len();
        if !&self.input[self.pos..].starts_with("\r\n") {
            return Err(Error::Incomplete);
        }
        self.pos += 2;

        let cmd = &cmd[2 + res.len()..];

        if cmd.len() < len {
            return Err(Error::Incomplete);
        }

        s.push_str(&cmd[0..len]);

        self.pos += len;
        self.pos += 2;

        Ok(Some(RESP::BulkString(Bytes::from(s))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Testcase {
        cmd: &'static str,
        resp: Result<RESP, Error>,
    }
    #[test]
    fn test_parse_string() {
        let testcases = vec![
            Testcase {
                cmd: ":0\r\n",
                resp: Ok(RESP::Integer(0)),
            },
            Testcase {
                cmd: ":-100\r\n",
                resp: Ok(RESP::Integer(-100)),
            },
            Testcase {
                cmd: "+OK\r\n",
                resp: Ok(RESP::SimpleString("OK".into())),
            },
            Testcase {
                cmd: "+OK\r",
                resp: Err(Error::Incomplete),
            },
            Testcase {
                cmd: "$5\r\nhello\r\n",
                resp: Ok(RESP::BulkString(Bytes::from("hello"))),
            },
            Testcase {
                cmd: "$5",
                resp: Err(Error::Incomplete),
            },
            Testcase {
                cmd: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
                resp: Ok(RESP::Array(vec![
                    RESP::BulkString(Bytes::from("ECHO")),
                    RESP::BulkString(Bytes::from("hey")),
                ])),
            },
        ];

        testcases.iter().for_each(|testcase| {
            let resp: Result<RESP, Error> = testcase.cmd.parse();
            assert_eq!(testcase.resp, resp);

            if let Ok(resp) = resp {
                let s: String = resp.into();
                assert_eq!(testcase.cmd, s);
            }
        })
    }
}
