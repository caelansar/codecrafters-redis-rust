use super::{
    consts::{self, op_code},
    encoding::Encoding,
    int_to_vec, read_exact,
};
use crate::protocol::RESP;
use std::mem;
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn verify_magic<R: AsyncRead + Unpin>(input: &mut R) -> anyhow::Result<()> {
    let mut magic = [0; 5];
    match input.read_exact(&mut magic).await {
        Ok(_) => (),
        Err(e) => anyhow::bail!(e),
    };

    if magic == consts::RDB_MAGIC.as_bytes() {
        Ok(())
    } else {
        anyhow::bail!("invalid magic string")
    }
}

pub async fn verify_version<R: AsyncRead + Unpin>(input: &mut R) -> anyhow::Result<()> {
    let mut version = [0; 4];
    match input.read_exact(&mut version).await {
        Ok(_) => (),
        Err(e) => anyhow::bail!(e),
    };

    let version = (version[0] - 48) as u32 * 1000
        + (version[1] - 48) as u32 * 100
        + (version[2] - 48) as u32 * 10
        + (version[3] - 48) as u32;

    let is_supported = version >= consts::SUPPORTED_MINIMUM && version <= consts::SUPPORTED_MAXIMUM;

    if is_supported {
        Ok(())
    } else {
        anyhow::bail!("version not supported")
    }
}

pub async fn read_length_with_encoding<R: AsyncRead + Unpin>(
    input: &mut R,
) -> anyhow::Result<(u32, bool)> {
    let length;
    let mut is_encoded = false;

    let enc_type = input.read_u8().await?;

    match (enc_type & 0xC0) >> 6 {
        consts::RDB_ENCVAL => {
            is_encoded = true;
            length = (enc_type & 0x3F) as u32;
        }
        consts::RDB_6BITLEN => {
            length = (enc_type & 0x3F) as u32;
        }
        consts::RDB_14BITLEN => {
            let next_byte = input.read_u8().await?;
            length = (((enc_type & 0x3F) as u32) << 8) | next_byte as u32;
        }
        _ => {
            length = input.read_u32().await?;
        }
    }

    Ok((length, is_encoded))
}

pub async fn read_length<R: AsyncRead + Unpin>(input: &mut R) -> anyhow::Result<u32> {
    let (length, _) = read_length_with_encoding(input).await?;
    Ok(length)
}

pub async fn read_blob<R: AsyncRead + Unpin>(input: &mut R) -> anyhow::Result<Vec<u8>> {
    let (length, is_encoded) = read_length_with_encoding(input).await?;

    if is_encoded {
        let encoding: Encoding = unsafe { mem::transmute(length) };

        let result = match encoding {
            Encoding::INT8 => int_to_vec(input.read_i8().await? as i32),
            Encoding::INT16 => int_to_vec(input.read_i16_le().await? as i32),
            Encoding::INT32 => int_to_vec(input.read_i32_le().await?),
            Encoding::LZF => {
                todo!()
            }
        };

        Ok(result)
    } else {
        read_exact(input, length as usize).await
    }
}

pub struct Parser<R: AsyncRead + Unpin> {
    input: R,
    last_expired: Option<u64>,
    kvs: Vec<(String, RESP)>,
}

impl<R: AsyncRead + Unpin> Parser<R> {
    pub fn new(input: R) -> Parser<R> {
        Parser {
            input,
            last_expired: None,
            kvs: vec![],
        }
    }

    pub fn get_kv_pairs(&self) -> impl Iterator<Item = &(String, RESP)> {
        return self.kvs.iter();
    }

    pub async fn parse(&mut self) -> anyhow::Result<()> {
        verify_magic(&mut self.input).await?;
        verify_version(&mut self.input).await?;

        let mut last_database: u32 = 0;

        loop {
            let next_op = self.input.read_u8().await?;

            match next_op {
                op_code::SELECTDB => {
                    last_database = read_length(&mut self.input).await.unwrap();
                    println!("database: {}", last_database);
                }
                op_code::EXPIRETIME_MS => todo!(),
                op_code::EXPIRETIME => todo!(),
                op_code::AUX => {
                    let auxkey = read_blob(&mut self.input).await?;
                    let auxval = read_blob(&mut self.input).await?;
                    println!("auxkey {:?}, auxval: {:?}", auxkey, auxval);
                }
                op_code::RESIZEDB => {
                    let db_size = read_length(&mut self.input).await?;
                    let expire_size = read_length(&mut self.input).await?;
                    println!("db_size {:?}, expire_size: {:?}", db_size, expire_size);
                }
                op_code::EOF => {
                    let mut checksum = Vec::new();
                    let _ = self.input.read_to_end(&mut checksum).await?;
                    break;
                }
                _ => {
                    let key = read_blob(&mut self.input).await?;

                    match next_op {
                        0 => {
                            // string type
                            let v = read_blob(&mut self.input).await?;

                            self.kvs.push((
                                String::from_utf8(key).unwrap(),
                                RESP::BulkString(String::from_utf8(v).ok()),
                            ));
                        }
                        _ => unimplemented!(),
                    }

                    self.last_expired = None;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::RESP::BulkString;
    use std::path::Path;
    use tokio::fs::File;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_parser() {
        let file = File::open(&Path::new("./src/rdb/tests/dump.rdb"))
            .await
            .unwrap();
        let reader = BufReader::new(file);

        let mut parser = Parser::new(reader);
        parser.parse().await.unwrap();

        assert_eq!(
            vec![
                (
                    "183358245".to_string(),
                    BulkString(Some("Positive 32 bit integer".into()))
                ),
                (
                    "125".to_string(),
                    BulkString(Some("Positive 8 bit integer".into()))
                ),
                (
                    "-29477".to_string(),
                    BulkString(Some("Negative 16 bit integer".into()))
                ),
                (
                    "-123".to_string(),
                    BulkString(Some("Negative 8 bit integer".into()))
                ),
                (
                    "43947".to_string(),
                    BulkString(Some("Positive 16 bit integer".into()))
                ),
                (
                    "-183358245".to_string(),
                    BulkString(Some("Negative 32 bit integer".into()))
                ),
            ],
            parser
                .get_kv_pairs()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<(String, RESP)>>()
        );
    }
}
