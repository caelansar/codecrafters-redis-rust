pub const RDB_MAGIC: &'static str = "REDIS";

pub const SUPPORTED_MINIMUM: u32 = 1;
pub const SUPPORTED_MAXIMUM: u32 = 7;

pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_ENCVAL: u8 = 3;

pub mod op_code {
    pub const AUX: u8 = 0xFA;
    pub const RESIZEDB: u8 = 0xFB;
    pub const EXPIRETIME_MS: u8 = 0xFC;
    pub const EXPIRETIME: u8 = 0xFD;
    pub const SELECTDB: u8 = 0xFE;
    pub const EOF: u8 = 0xFF;
}
