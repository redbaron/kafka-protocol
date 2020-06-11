use uuid;

// Kafka protocol primitive types
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
    fn emit_array<T>(&mut self, v: Option<&[T]>) -> Result<Self::Ok, Self::Error>
        where T: super::KafkaProtoEncodable;
    fn emit_compact_array<T>(&mut self, v: Option<&[T]>) -> Result<Self::Ok, Self::Error>
        where T: super::KafkaProtoEncodable;
}