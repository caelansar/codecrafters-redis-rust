use crate::parse::Parse;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct Ping {}

impl Ping {
    pub fn new() -> Ping {
        Ping {}
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Ping> {
        parse.finish()?;
        Ok(Ping {})
    }
}
