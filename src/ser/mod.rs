use std::io::Write;

use crate::error::{Error, Result};

use std::convert::TryInto;

mod varint;

pub struct KafkaSerializer<W> {
    writer: W,
}

impl<W: Write> KafkaSerializer<W> {
    fn emit_varuint(&mut self, v: u32) -> Result<()> {
        let mut buf = [0u8; 5];
        let count = varint::encode_varint32(v, &mut buf);
        self.writer.write_all(&buf[0..count]).map_err(Into::into)
    }

    fn emit_varulong(&mut self, v: u64) -> Result<()> {
        let mut buf = [0u8; 10];
        let count = varint::encode_varint64(v, &mut buf);
        self.writer.write_all(&buf[0..count]).map_err(Into::into)
    }
}

impl<W: Write> super::KafkaProtoEncoder for KafkaSerializer<W> {
    type Ok = ();
    type Error = Error;

    // KF: BOOL
    fn emit_bool(&mut self, v: bool) -> Result<()> {
        self.writer
            .write_all(&(v as u8).to_be_bytes())
            .map_err(Into::into)
    }
    // KF: INT8
    fn emit_int8(&mut self, v: i8) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: INT16
    fn emit_int16(&mut self, v: i16) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: INT32
    fn emit_int32(&mut self, v: i32) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: INT64
    fn emit_int64(&mut self, v: i64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: UINT32
    fn emit_uint32(&mut self, v: u32) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: VARINT
    fn emit_varint(&mut self, v: i32) -> Result<()> {
        self.emit_varuint(varint::encode_zig_zag_32(v))
    }

    // KF: VARLONG
    fn emit_varlong(&mut self, v: i64) -> Result<()> {
        self.emit_varulong(varint::encode_zig_zag_64(v))
    }

    fn emit_uuid(&mut self, _v: uuid::Uuid) -> Result<()> {
        unimplemented!()
    }

    // KF: FLOAT64
    fn emit_float64(&mut self, v: f64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: STRING
    fn emit_string(&mut self, v: &str) -> Result<()> {
        self.emit_int16(v.len().try_into()?)?;
        self.writer.write_all(&v.as_bytes()).map_err(Into::into)
    }

    // KF: COMPACT_STRING
    fn emit_compact_string(&mut self, v: &str) -> Result<()> {
        self.emit_varuint(v.len() as u32)?;
        self.writer.write_all(v.as_bytes()).map_err(Into::into)
    }

    // KF: NULLABLE_STRING
    fn emit_nullable_string(&mut self, v: Option<&str>) -> Result<()> {
        match v {
            Some(s) => self.emit_string(s),
            None => self.emit_int16(-1),
        }
    }

    // KF: COMPACT_NULLABLE_STRING
    fn emit_compact_nullable_string(&mut self, v: Option<&str>) -> Result<()> {
        match v {
            Some(s) => self.emit_compact_string(s),
            None => self.emit_varint(0),
        }
    }

    // KF: BYTES
    fn emit_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.emit_int32(v.len().try_into()?)?;
        self.writer.write_all(v).map_err(Into::into)
    }

    // KF: COMPACT_BYTES
    fn emit_compact_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.emit_varuint(v.len().try_into()?)?;
        self.writer.write_all(v).map_err(Into::into)
    }

    // KF: NULLABLE_BYTES
    fn emit_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<()> {
        match v {
            Some(s) => {
                self.emit_int32(s.len().try_into()?)?;
                self.writer.write_all(s).map_err(Into::into)
            }
            None => self.emit_int32(-1),
        }
    }

    // KF: COMPACT_BYTES
    fn emit_compact_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<()> {
        match v {
            Some(s) => {
                self.emit_varuint(s.len().try_into()?)?;
                self.writer.write_all(s).map_err(Into::into)
            }
            None => self.emit_varuint(0),
        }
    }

    fn emit_array<'a, T: 'a>(&mut self, v: impl ExactSizeIterator<Item = &'a T>) -> Result<()>
    where
        T: super::KafkaProtoEncodable,
    {
        if v.len() > 0 {
            self.emit_int32(v.len().try_into()?)?;
            for x in v {
                x.emit(self)?;
            }
            Ok(())
        } else {
            self.emit_int32(-1)
        }
    }

    fn emit_compact_array<'a, T: 'a>(
        &mut self,
        v: impl ExactSizeIterator<Item = &'a T>,
    ) -> Result<()>
    where
        T: super::KafkaProtoEncodable,
    {
        if v.len() > 0 {
            self.emit_varuint(v.len().try_into()?)?;
            for x in v {
                x.emit(self)?;
            }
            Ok(())
        } else {
            self.emit_varuint(0)
        }
    }
}
