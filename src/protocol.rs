use std::str::FromStr;

const CR: &str = "\r";
const LF: &str = "\n";

// Redis Serialization Protocol
#[derive(Debug, PartialOrd, PartialEq, Clone)]
pub enum RESP {
    // Simple strings are encoded as a plus (+) character, followed by a string.
    // The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
    SimpleString(String),
    // A bulk string represents a single binary string. The string can be of any size,
    // but by default, Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
    BulkString(Option<String>),
    // Clients send commands to the Redis server as RESP arrays. Similarly, some Redis commands that
    // return collections of elements use arrays as their replies. An example is the LRANGE command that returns elements of a list.
    Array(Vec<RESP>),
    // The null data type represents non-existent values.
    Null,
}

impl FromStr for RESP {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parser = Decoder::new(s);
        parser.parse().ok_or("parse failed")
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
            Self::BulkString(s) => {
                if s.is_none() {
                    res.push_str("$-1\r\n");
                } else {
                    let s = s.as_ref().unwrap();
                    res.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
                }
            }
            Self::Array(v) => {
                res.push_str(&format!("*{}\r\n", v.len()));

                v.iter().for_each(|r| {
                    res.push_str(r.encode().as_str());
                })
            }
            Self::Null => {
                res.push_str("_\r\n");
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
    pub(crate) fn parse(&mut self) -> Option<RESP> {
        if self.pos >= self.input.len() {
            return None;
        }

        let cmd = &self.input[self.pos..];
        println!("cmd: {}", cmd);

        match cmd.as_bytes()[0] {
            b'*' => {
                self.pos += 1;
                self.parse_array()
            }
            b'$' => {
                self.pos += 1;
                self.parse_bulk_string()
            }
            b'+' => {
                self.pos += 1;
                self.parse_simple_string()
            }
            _ => unreachable!(),
        }
    }
    fn parse_array(&mut self) -> Option<RESP> {
        if self.pos >= self.input.len() {
            return None;
        }

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();
        let item_count: usize = res.parse().unwrap();

        self.pos += res.len();
        self.pos += 2;

        let mut arr = Vec::with_capacity(item_count);

        (0..item_count).for_each(|_| {
            let item = self.parse();
            if let Some(item) = item {
                arr.push(item);
            }
        });

        Some(RESP::Array(arr))
    }

    fn parse_simple_string(&mut self) -> Option<RESP> {
        if self.pos >= self.input.len() {
            return None;
        }

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();

        self.pos += res.len() + 2;

        Some(RESP::SimpleString(res))
    }

    fn parse_bulk_string(&mut self) -> Option<RESP> {
        if self.pos >= self.input.len() {
            return None;
        }
        let mut s = String::new();

        let cmd = &self.input[self.pos..];

        let res = cmd.chars().take_while(|x| *x != '\r').collect::<String>();
        let len: usize = res.parse().unwrap();

        self.pos += 2;
        self.pos += res.len();

        let cmd = &cmd[2 + res.len()..];

        s.push_str(&cmd[0..len]);

        self.pos += len;
        self.pos += 2;

        Some(RESP::BulkString(Some(s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Testcase {
        cmd: &'static str,
        resp: RESP,
    }
    #[test]
    fn test_parse_string() {
        let testcases = vec![
            Testcase {
                cmd: "+OK\r\n",
                resp: RESP::SimpleString("OK".into()),
            },
            Testcase {
                cmd: "$5\r\nhello\r\n",
                resp: RESP::BulkString(Some("hello".into())),
            },
            Testcase {
                cmd: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
                resp: RESP::Array(vec![
                    RESP::BulkString(Some("ECHO".into())),
                    RESP::BulkString(Some("hey".into())),
                ]),
            },
        ];

        testcases.iter().for_each(|testcase| {
            let resp: RESP = testcase.cmd.parse().unwrap();
            assert_eq!(testcase.resp, resp);

            let s: String = resp.into();
            assert_eq!(testcase.cmd, s);
        })
    }
}
