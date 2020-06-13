use std::io::Write;

use crate::error::{Error, Result};

use std::convert::TryInto;

mod varint;

struct KafkaSerializer<W> {
    writer: W,
}

impl<W: Write> KafkaSerializer<W> {
    fn new(writer: W) -> KafkaSerializer<W> {
        KafkaSerializer { writer }
    }

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

    fn emit_array_hdr(&mut self, len: usize) -> Result<()> {
        if len > 0 {
            self.emit_int32(len.try_into()?)
        } else {
            self.emit_int32(-1)
        }
    }

    fn emit_compact_array_hdr(&mut self, len: usize) -> Result<()> {
        if len > 0 {
            self.emit_varuint(len.try_into()?)
        } else {
            self.emit_varuint(0)
        }
    }
}

pub struct KafkaFlexiSerializer<T> {
    use_flexi: bool,
    s: KafkaSerializer<T>,
}

impl<W: Write> KafkaFlexiSerializer<W> {
    pub fn new(version: i16, flexible_version: i16, writer: W) -> KafkaFlexiSerializer<W> {
        KafkaFlexiSerializer {
            use_flexi: version >= flexible_version,
            s: KafkaSerializer::new(writer),
        }
    }
}

use crate::encoder::KafkaProtoEncoder;
impl<W: Write> crate::KafkaFlexibleEncoder for KafkaFlexiSerializer<W> {
    type Ok = ();
    type Error = Error;

    fn emit_bool(&mut self, v: bool) -> Result<()> {
        self.s.emit_bool(v)
    }
    fn emit_int8(&mut self, v: i8) -> Result<()> {
        self.s.emit_int8(v)
    }
    fn emit_int16(&mut self, v: i16) -> Result<()> {
        self.s.emit_int16(v)
    }

    fn emit_int32(&mut self, v: i32) -> Result<()> {
        self.s.emit_int32(v)
    }

    fn emit_int64(&mut self, v: i64) -> Result<()> {
        self.s.emit_int64(v)
    }

    fn emit_uint32(&mut self, v: u32) -> Result<()> {
        self.s.emit_uint32(v)
    }

    fn emit_varint(&mut self, v: i32) -> Result<()> {
        self.s.emit_varint(v)
    }

    fn emit_varlong(&mut self, v: i64) -> Result<()> {
        self.s.emit_varlong(v)
    }

    fn emit_uuid(&mut self, v: uuid::Uuid) -> Result<()> {
        self.s.emit_uuid(v)
    }

    fn emit_float64(&mut self, v: f64) -> Result<()> {
        self.s.emit_float64(v)
    }

    fn emit_string(&mut self, v: &str) -> Result<()> {
        if self.use_flexi {
            self.s.emit_compact_string(v)
        } else {
            self.s.emit_string(v)
        }
    }

    fn emit_nullable_string(&mut self, v: Option<&str>) -> Result<()> {
        if self.use_flexi {
            self.s.emit_compact_nullable_string(v)
        } else {
            self.s.emit_nullable_string(v)
        }
    }

    fn emit_bytes(&mut self, v: &[u8]) -> Result<()> {
        if self.use_flexi {
            self.s.emit_compact_bytes(v)
        } else {
            self.s.emit_bytes(v)
        }
    }

    fn emit_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<()> {
        if self.use_flexi {
            self.s.emit_compact_nullable_bytes(v)
        } else {
            self.s.emit_nullable_bytes(v)
        }
    }
    //fn emit_records(&mut self, v: &[Records]) -> Result<()>;
    fn emit_array<'a, T: 'a>(&mut self, v: impl ExactSizeIterator<Item = &'a T>) -> Result<()>
    where
        T: super::KafkaProtoEncodable,
    {
        if self.use_flexi {
            self.s.emit_compact_array_hdr(v.len())?;
        } else {
            self.s.emit_array_hdr(v.len())?;
        };

        for e in v {
            e.emit(self)?;
        }
        Ok(())
    }
}
