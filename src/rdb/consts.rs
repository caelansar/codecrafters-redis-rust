pub const RDB_MAGIC: &str = "REDIS";

pub const SUPPORTED_MINIMUM: u32 = 1;
pub const SUPPORTED_MAXIMUM: u32 = 7;

pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_ENCVAL: u8 = 3;
pub const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub mod op_code {
    pub const AUX: u8 = 0xFA;
    pub const RESIZEDB: u8 = 0xFB;
    pub const EXPIRETIME_MS: u8 = 0xFC;
    pub const EXPIRETIME: u8 = 0xFD;
    pub const SELECTDB: u8 = 0xFE;
    pub const EOF: u8 = 0xFF;
}
