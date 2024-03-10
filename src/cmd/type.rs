use crate::parse::Parse;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn new(key: impl ToString) -> Type {
        Type {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Type> {
        let key = parse.next_string()?;

        Ok(Type { key })
    }
}
