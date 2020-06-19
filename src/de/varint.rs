use crate::error::Error;

pub(crate) fn decode_zig_zag_32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

pub(crate) fn decode_zig_zag_64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

pub(crate) fn decode_varint64(buf: &[u8]) -> Result<u64, Error> {
    let mut r: u64 = 0;
    let mut i = 0;

    for &b in buf {
        if i == 10 {
            return Err(Error::IncorrectVarint);
        }
        // TODO: may overflow if i == 9
        r = r | (((b & 0x7f) as u64) << (i * 7));
        i += 1;
        if b < 0x80 {
            return Ok(r);
        }
    }
    return Err(Error::IncorrectVarint);
}
