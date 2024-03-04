use crate::parse::Parse;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Echo {
    message: String,
}

impl Echo {
    pub fn new(message: impl ToString) -> Echo {
        Echo {
            message: message.to_string(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Echo> {
        let message = parse.next_string()?;

        Ok(Echo { message })
    }
}
