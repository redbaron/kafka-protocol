// SOURCE: https://github.com/stepancheg/rust-protobuf/blob/68c7a5a5d42912120d0af56d4bf5e946bad1da34/protobuf/src/varint.rs

#[inline]
pub(crate) fn encode_varint64(mut value: u64, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= 10);

    fn iter(value: &mut u64, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    if !iter(&mut value, &mut buf[4]) {
        return 5;
    };
    if !iter(&mut value, &mut buf[5]) {
        return 6;
    };
    if !iter(&mut value, &mut buf[6]) {
        return 7;
    };
    if !iter(&mut value, &mut buf[7]) {
        return 8;
    };
    if !iter(&mut value, &mut buf[8]) {
        return 9;
    };
    buf[9] = value as u8;
    10
}

/// Encode u32 value as varint.
/// Panics if buffer length is less than 5.
#[inline]
pub(crate) fn encode_varint32(mut value: u32, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= 5);

    fn iter(value: &mut u32, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    buf[4] = value as u8;
    5
}

pub(crate) fn encode_zig_zag_32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

pub(crate) fn encode_zig_zag_64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}
