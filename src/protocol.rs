use std::str::FromStr;

const CRLF: &'static str = "\r\n";

// Redis Serialization Protocol
#[derive(Debug, PartialOrd, PartialEq)]
pub enum RESP {
    // Simple strings are encoded as a plus (+) character, followed by a string.
    // The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
    SimpleString(String),
    // A bulk string represents a single binary string. The string can be of any size,
    // but by default, Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
    BulkString(String),
    // Clients send commands to the Redis server as RESP arrays. Similarly, some Redis commands that
    // return collections of elements use arrays as their replies. An example is the LRANGE command that returns elements of a list.
    Array(Vec<RESP>),
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
                res.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
            }
            Self::Array(v) => {
                res.push_str(&format!("*{}\r\n", v.len()));

                v.iter().for_each(|r| {
                    res.push_str(r.encode().as_str());
                })
            }
        }
        res
    }
}

struct Decoder<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a str) -> Decoder<'a> {
        Decoder { input, pos: 0 }
    }
    fn parse(&mut self) -> Option<RESP> {
        if self.pos >= self.input.len() {
            return None;
        }

        let cmd = &self.input[self.pos..];

        match cmd.as_bytes()[0] {
            b'*' => {
                self.pos += 1;
                self.parse_array()
            }
            b'$' => {
                self.pos += 1;
                self.parse_bulk_string()
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

        Some(RESP::BulkString(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string() {
        let cmd = "$5\r\nhello\r\n";

        let resp: RESP = cmd.parse().unwrap();

        assert_eq!(RESP::BulkString("hello".into()), resp);

        let s: String = resp.into();
        assert_eq!(cmd, s);
    }

    #[test]
    fn test_parse_array() {
        let cmd = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";

        let resp: RESP = cmd.parse().unwrap();

        assert_eq!(
            RESP::Array(vec![
                RESP::BulkString("ECHO".into()),
                RESP::BulkString("hey".into())
            ]),
            resp
        );

        let s: String = resp.into();
        assert_eq!(cmd, s);
    }
}
