// Low level Kafka protocol primitive types
pub trait KafkaProtoDecodable {
    fn deserialize<'a, D: KafkaFlexibleDecoder<'a>>(
        version: i16,
        d: &mut D,
    ) -> Result<Self, D::Error>
    where
        Self: Sized;
}
// Ref: https://kafka.apache.org/protocol#protocol_types
pub trait KafkaProtoDecoder<'de> {
    type Error;

    fn read_bool(&mut self) -> Result<bool, Self::Error>;
    fn read_int8(&mut self) -> Result<i8, Self::Error>;
    fn read_int16(&mut self) -> Result<i16, Self::Error>;
    fn read_int32(&mut self) -> Result<i32, Self::Error>;
    fn read_int64(&mut self) -> Result<i64, Self::Error>;
    fn read_uint32(&mut self) -> Result<u32, Self::Error>;
    fn read_varint(&mut self) -> Result<i32, Self::Error>;
    fn read_varlong(&mut self) -> Result<i64, Self::Error>;
    fn read_uuid(&mut self) -> Result<uuid::Uuid, Self::Error>;

    fn read_float64(&mut self) -> Result<f64, Self::Error>;
    fn read_string(&mut self) -> Result<&'de str, Self::Error>;
    fn read_compact_string(&mut self) -> Result<&'de str, Self::Error>;
    fn read_nullable_string(&mut self) -> Result<Option<&'de str>, Self::Error>;
    fn read_compact_nullable_string(&mut self) -> Result<Option<&'de str>, Self::Error>;
    fn read_bytes(&mut self) -> Result<&'de [u8], Self::Error>;
    fn read_compact_bytes(&mut self) -> Result<&'de [u8], Self::Error>;
    fn read_nullable_bytes(&mut self) -> Result<Option<&'de [u8]>, Self::Error>;
    fn read_compact_nullable_bytes(&mut self) -> Result<Option<&'de [u8]>, Self::Error>;
    //fn read_records(&mut self) -> Result< &[Records], Self::Error>;
    fn read_array_hdr(&mut self) -> Result<usize, Self::Error>;
    fn read_compact_array_hdr(&mut self) -> Result<usize, Self::Error>;
}

pub trait KafkaFlexibleDecoder<'de> {
    type Error;

    fn read_bool(&mut self) -> Result<bool, Self::Error>;
    fn read_int8(&mut self) -> Result<i8, Self::Error>;
    fn read_int16(&mut self) -> Result<i16, Self::Error>;
    fn read_int32(&mut self) -> Result<i32, Self::Error>;
    fn read_int64(&mut self) -> Result<i64, Self::Error>;
    fn read_uint32(&mut self) -> Result<u32, Self::Error>;
    fn read_varint(&mut self) -> Result<i32, Self::Error>;
    fn read_varlong(&mut self) -> Result<i64, Self::Error>;
    fn read_uuid(&mut self) -> Result<uuid::Uuid, Self::Error>;

    fn read_float64(&mut self) -> Result<f64, Self::Error>;
    fn read_string(&mut self) -> Result<&'de str, Self::Error>;
    fn read_nullable_string(&mut self) -> Result<Option<&'de str>, Self::Error>;
    fn read_bytes(&mut self) -> Result<&'de [u8], Self::Error>;
    fn read_nullable_bytes(&mut self) -> Result<Option<&'de [u8]>, Self::Error>;
    //fn emit_records(&mut self) -> Result< &[Records], Self::Error>;
    fn read_array_hdr(&mut self) -> Result<usize, Self::Error>;
    //        T: KafkaProtoDecodable;
}
