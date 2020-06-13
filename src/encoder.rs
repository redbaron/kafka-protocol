use std::io;
use std::iter::ExactSizeIterator;
use uuid;

// Kafka protocol message
// Messages always start with header containing API_KEY and message version,
// followed by serialized message content.
// `serialize` method should not be serializing header, only message content.
pub trait KafkaProtoMessage {
    fn serialize<S: KafkaFlexibleEncoder>(
        &self,
        version: i16,
        s: &mut S,
    ) -> Result<S::Ok, S::Error>;
}

pub trait KafkaProtoEncodable {
    fn emit<S: KafkaFlexibleEncoder>(&self, s: &mut S) -> Result<S::Ok, S::Error>;
}

// Low level Kafka protocol primitive types
// Ref: https://kafka.apache.org/protocol#protocol_types
pub trait KafkaProtoEncoder {
    type Ok;
    type Error;

    fn emit_bool(&mut self, v: bool) -> Result<Self::Ok, Self::Error>;
    fn emit_int8(&mut self, v: i8) -> Result<Self::Ok, Self::Error>;
    fn emit_int16(&mut self, v: i16) -> Result<Self::Ok, Self::Error>;
    fn emit_int32(&mut self, v: i32) -> Result<Self::Ok, Self::Error>;
    fn emit_int64(&mut self, v: i64) -> Result<Self::Ok, Self::Error>;
    fn emit_uint32(&mut self, v: u32) -> Result<Self::Ok, Self::Error>;
    fn emit_varint(&mut self, v: i32) -> Result<Self::Ok, Self::Error>;
    fn emit_varlong(&mut self, v: i64) -> Result<Self::Ok, Self::Error>;
    fn emit_uuid(&mut self, v: uuid::Uuid) -> Result<Self::Ok, Self::Error>;

    fn emit_float64(&mut self, v: f64) -> Result<Self::Ok, Self::Error>;
    fn emit_string(&mut self, v: &str) -> Result<Self::Ok, Self::Error>;
    fn emit_compact_string(&mut self, v: &str) -> Result<Self::Ok, Self::Error>;
    fn emit_nullable_string(&mut self, v: Option<&str>) -> Result<Self::Ok, Self::Error>;
    fn emit_compact_nullable_string(&mut self, v: Option<&str>) -> Result<Self::Ok, Self::Error>;
    fn emit_bytes(&mut self, v: &[u8]) -> Result<Self::Ok, Self::Error>;
    fn emit_compact_bytes(&mut self, v: &[u8]) -> Result<Self::Ok, Self::Error>;
    fn emit_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<Self::Ok, Self::Error>;
    fn emit_compact_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<Self::Ok, Self::Error>;
    //fn emit_records(&mut self, v: &[Records]) -> Result<Self::Ok, Self::Error>;
    fn emit_array_hdr(&mut self, len: usize) -> Result<Self::Ok, Self::Error>;
    fn emit_compact_array_hdr(&mut self, len: usize) -> Result<Self::Ok, Self::Error>;
}

// Encoder, which automatically selects implementation for bytes,strings and arrays,
// which can be potentially flexi-coded
pub trait KafkaFlexibleEncoder {
    type Ok;
    type Error;

    fn emit_bool(&mut self, v: bool) -> Result<Self::Ok, Self::Error>;
    fn emit_int8(&mut self, v: i8) -> Result<Self::Ok, Self::Error>;
    fn emit_int16(&mut self, v: i16) -> Result<Self::Ok, Self::Error>;
    fn emit_int32(&mut self, v: i32) -> Result<Self::Ok, Self::Error>;
    fn emit_int64(&mut self, v: i64) -> Result<Self::Ok, Self::Error>;
    fn emit_uint32(&mut self, v: u32) -> Result<Self::Ok, Self::Error>;
    fn emit_varint(&mut self, v: i32) -> Result<Self::Ok, Self::Error>;
    fn emit_varlong(&mut self, v: i64) -> Result<Self::Ok, Self::Error>;
    fn emit_uuid(&mut self, v: uuid::Uuid) -> Result<Self::Ok, Self::Error>;

    fn emit_float64(&mut self, v: f64) -> Result<Self::Ok, Self::Error>;
    fn emit_string(&mut self, v: &str) -> Result<Self::Ok, Self::Error>;
    fn emit_nullable_string(&mut self, v: Option<&str>) -> Result<Self::Ok, Self::Error>;
    fn emit_bytes(&mut self, v: &[u8]) -> Result<Self::Ok, Self::Error>;
    fn emit_nullable_bytes(&mut self, v: Option<&[u8]>) -> Result<Self::Ok, Self::Error>;
    //fn emit_records(&mut self, v: &[Records]) -> Result<Self::Ok, Self::Error>;
    fn emit_array<'a, T: 'a>(
        &mut self,
        v: impl ExactSizeIterator<Item = &'a T>,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: KafkaProtoEncodable;
}

use crate::messages::RequestHeader;
use crate::ser;
use crate::Request;

pub fn serialize_message<M, W>(
    m: &M,
    ver: i16,
    client_id: &str,
    w: &mut W,
) -> crate::error::Result<()>
where
    M: KafkaProtoMessage + Request,
    W: io::Write,
{
    let hdr = RequestHeader {
        request_api_key: M::API_KEY,
        request_api_version: ver,
        correlation_id: 0,
        client_id: client_id,
    };

    let is_flexible = ver >= M::FLEXIBLE_VERSION;

    // Ref: https://github.com/apache/kafka/blob/2.5.0/generator/src/main/java/org/apache/kafka/message/ApiMessageTypeGenerator.java#L252-L318
    let hdr_ver = if is_flexible { 2i16 } else { 1i16 };

    let mut s = ser::KafkaFlexiSerializer::new(is_flexible, w);

    hdr.serialize(hdr_ver, &mut s.serializer)?;
    m.serialize(ver, &mut s)
}
