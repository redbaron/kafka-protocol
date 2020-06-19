use std::convert::TryInto;

use crate::error::Error;

mod decoders;
mod varint;

pub use decoders::*;

pub struct KafkaDeserializer<'a> {
    buf: &'a [u8],
    i: usize,
}

macro_rules! read_n {
    ($self:expr,$x:expr) => {{
        let i = $self.i;
        //$self.i += TryInto::<usize>::try_into($x)?;
        $self.i += $x;
        $self.buf.get(i..$self.i).ok_or(Error::OutOfBounds)?
    }};
}

macro_rules! read_primitive {
    ($self:expr,$x:ty) => {{
        <$x>::from_be_bytes(
            read_n!($self, std::mem::size_of::<$x>())
                .try_into()
                .unwrap(),
        )
    }};
}

impl KafkaDeserializer<'_> {
    pub fn new(buf: &[u8]) -> KafkaDeserializer {
        KafkaDeserializer { buf, i: 0 }
    }

    fn read_varuint(&mut self) -> Result<u32, Error> {
        let buf: [u8; 5] = read_n!(self, 5)
            .try_into()
            .map_err(|_| Error::OutOfBounds)?;
        Ok(varint::decode_varint64(&buf)? as u32)
    }

    fn read_varulong(&mut self) -> Result<u64, Error> {
        let buf: [u8; 10] = read_n!(self, 10)
            .try_into()
            .map_err(|_| Error::OutOfBounds)?;
        varint::decode_varint64(&buf)
    }
}

impl<'de, 'a: 'de> KafkaProtoDecoder<'de> for KafkaDeserializer<'a> {
    type Error = Error;

    fn read_bool(&mut self) -> Result<bool, Self::Error> {
        Ok(read_primitive!(self, u8) != 0)
    }
    fn read_int8(&mut self) -> Result<i8, Self::Error> {
        Ok(read_primitive!(self, i8))
    }

    fn read_int16(&mut self) -> Result<i16, Self::Error> {
        Ok(read_primitive!(self, i16))
    }
    fn read_int32(&mut self) -> Result<i32, Self::Error> {
        Ok(read_primitive!(self, i32))
    }
    fn read_int64(&mut self) -> Result<i64, Self::Error> {
        Ok(read_primitive!(self, i64))
    }

    fn read_uint32(&mut self) -> Result<u32, Self::Error> {
        Ok(read_primitive!(self, u32))
    }

    fn read_varint(&mut self) -> Result<i32, Self::Error> {
        Ok(varint::decode_zig_zag_32(self.read_varuint()?))
    }

    fn read_varlong(&mut self) -> Result<i64, Self::Error> {
        Ok(varint::decode_zig_zag_64(self.read_varulong()?))
    }

    fn read_uuid(&mut self) -> Result<uuid::Uuid, Self::Error> {
        unimplemented!()
    }

    fn read_float64(&mut self) -> Result<f64, Self::Error> {
        Ok(read_primitive!(self, f64))
    }

    fn read_string(&mut self) -> Result<&'de str, Self::Error> {
        let len: usize = self.read_int16()?.try_into()?;
        std::str::from_utf8(read_n!(self, len)).map_err(Into::into)
    }

    fn read_compact_string(&mut self) -> Result<&'de str, Self::Error> {
        let len: usize = self.read_varuint()?.try_into()?;
        std::str::from_utf8(read_n!(self, len)).map_err(Into::into)
    }

    fn read_nullable_string(&mut self) -> Result<Option<&'de str>, Self::Error> {
        let len: i16 = self.read_int16()?;
        if len == (-1) {
            Ok(None)
        } else {
            let ulen: usize = len.try_into()?;
            Ok(Some(std::str::from_utf8(read_n!(self, ulen))?))
        }
    }

    fn read_compact_nullable_string(&mut self) -> Result<Option<&'de str>, Self::Error> {
        let len: usize = self.read_varuint()?.try_into()?;
        if len == (0) {
            Ok(None)
        } else {
            Ok(Some(std::str::from_utf8(read_n!(self, len))?))
        }
    }

    fn read_bytes(&mut self) -> Result<&'de [u8], Self::Error> {
        let len: usize = self.read_int32()?.try_into()?;
        Ok(read_n!(self, len))
    }

    fn read_compact_bytes(&mut self) -> Result<&'de [u8], Self::Error> {
        let len: usize = self.read_varuint()?.try_into()?;
        Ok(read_n!(self, len))
    }

    fn read_nullable_bytes(&mut self) -> Result<Option<&'de [u8]>, Self::Error> {
        let len: i32 = self.read_int32()?;
        if len == (-1) {
            Ok(None)
        } else {
            let ulen: usize = len.try_into()?;
            Ok(Some(read_n!(self, ulen)))
        }
    }

    fn read_compact_nullable_bytes(&mut self) -> Result<Option<&'de [u8]>, Self::Error> {
        let len: usize = self.read_varuint()?.try_into()?;
        if len == 0 {
            Ok(None)
        } else {
            Ok(Some(read_n!(self, len)))
        }
    }

    //fn read_records(&mut self) -> Result< &[Records], Self::Error>;

    fn read_array_hdr(&mut self) -> Result<usize, Self::Error> {
        let len = self.read_int32()?;
        if len == (-1) {
            Ok(0)
        } else {
            len.try_into().map_err(Into::into)
        }
    }
    fn read_compact_array_hdr(&mut self) -> Result<usize, Self::Error> {
        self.read_varuint()?.try_into().map_err(Into::into)
    }
}
