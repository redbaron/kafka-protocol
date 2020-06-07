use std::io::Write;

use crate::error::{Error, Result};
use serde::{ser, Serialize};

use crate::Bytes;
//use varint;

mod varint;

pub struct KafkaSerializer<W> {
    version: i16, //protocol version
    writer: W,
}

pub fn to_bytes<T>(version: i16, value: &T) -> Result<Bytes>
where
    T: Serialize,
{
    let mut s = KafkaSerializer {
        version,
        writer: Vec::new(),
    };
    value.serialize(&mut s)?;
    Ok(s.writer)
}

impl<W: Write> KafkaSerializer<W> {
    // // KF: VARUINT (not officially present, but mentioned as part of COMPACT_STRING)
    // fn serialize_varuint(&mut self, v: u32) -> Result<()> {
    //     let mut buf: [u8; 5];
    //     self.writer
    //         .write_all(&buf[0..varint::encode_varint32(v, &mut buf)])
    //         .map_err(Into::into)
    // }

    // // KF: VARINT
    // fn serialize_varint(&mut self, v: i32) -> Result<()> {
    //     self.serialize_varuint(varint::encode_zig_zag_32(v))
    // }

    // fn serialize_varulong(&mut self, v: u64) -> Result<()> {
    //     let mut buf: [u8; 10];
    //     self.writer
    //         .write_all(&buf[0..varint::encode_varint64(v, &mut buf)])
    //         .map_err(Into::into)
    // }

    // // KF: VARLONG
    // fn serialize_varlong(&mut self, v: i64) -> Result<()> {
    //     self.serialize_varulong(varint::encode_zig_zag_64(v))
    // }

    // // KF: COMPACT_STRING
    // fn serialize_compact_string(&mut self, v: &str) -> Result<()> {
    //     self.serialize_varuint(v.len() as u32)?;
    //     self.writer.write_all(v.as_bytes()).map_err(Into::into)
    // }

    // // KF: NULLABLE_STRING
    // fn serialize_nullable_string(&mut self, v: Option<&str>) -> Result<()> {
    //     match v {
    //         Some(s) => self.serialize_str(s),
    //         None => self.serialize_i16(-1),
    //     }
    // }

    // fn serialize_compact_nullable_string(&mut self, v: Option<&str>) -> Result<()> {
    //     match v {
    //         Some(s) => self.serialize_compact_string(v),
    //         None => self.serialize_varint(0),
    //     }
    // }

    // // KF: COMPACT_BYTES
    // fn serialize_compact_bytes(&mut self, v: &[u8]) -> Result<()> {
    //     self.serialize_varuint(v.len() as u32)?;
    //     self.writer.write_all(v).map_err(Into::into)
    // }

    // // KF: NULLABLE_BYTES
    // // fn serialize_nullable_bytes(&mut self, v: &[u8]) -> Result<()> {
    // //     self.
    // // }
}

impl<'a, W: Write> ser::Serializer for &'a mut KafkaSerializer<W> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    // KF: BOOL
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.writer
            .write_all(&(v as u8).to_be_bytes())
            .map_err(Into::into)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u32(v as u32)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u32(v as u32)
    }

    // KF: UINT32
    fn serialize_u32(self, v: u32) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        Err(Error::Unsupported("u64"))
    }

    // KF: INT8
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: INT16
    fn serialize_i16(self, v: i16) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: INT32
    fn serialize_i32(self, v: i32) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: INT64
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(v as f64)
    }

    // KF: FLOAT64
    fn serialize_f64(self, v: f64) -> Result<()> {
        self.writer.write_all(&v.to_be_bytes()).map_err(Into::into)
    }

    // KF: STRING
    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_i16(v.len() as i16)?;
        self.writer.write_all(&v.as_bytes()).map_err(Into::into)
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_str(v.encode_utf8(&mut [0 as u8; 4]))
    }

    // KF: BYTES
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.serialize_i32(v.len() as i32)?;
        self.writer.write_all(v).map_err(Into::into)
    }
}
