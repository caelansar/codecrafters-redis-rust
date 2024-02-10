use std::time::Instant;

pub(crate) struct Entry {
    pub(crate) val: String,
    pub(crate) exp: Option<Instant>,
}
