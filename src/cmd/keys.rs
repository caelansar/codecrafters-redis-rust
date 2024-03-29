use crate::parse::Parse;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Keys {
    pattern: String,
}

#[allow(unused)]
impl Keys {
    pub fn new(pattern: impl ToString) -> Keys {
        Keys {
            pattern: pattern.to_string(),
        }
    }

    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Keys> {
        let message = parse.next_string()?;

        Ok(Keys { pattern: message })
    }
}
