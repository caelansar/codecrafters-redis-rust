use std::time::{Instant, SystemTime};

pub(crate) struct Entry {
    pub(crate) val: String,
    pub(crate) exp: Option<SystemTime>,
}
