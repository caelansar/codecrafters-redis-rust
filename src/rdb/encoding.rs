#[repr(u32)]
#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub enum Encoding {
    INT8 = 0,
    INT16 = 1,
    INT32 = 2,
    LZF = 3,
}
