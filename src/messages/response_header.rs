use crate::{de::KafkaProtoDecoder, ser::KafkaProtoEncoder};

pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn serialize<S: KafkaProtoEncoder>(&self, _ver: i16, s: &mut S) -> Result<S::Ok, S::Error> {
        s.emit_int32(self.correlation_id)
    }

    pub fn deserialize<'a, D: KafkaProtoDecoder<'a>>(
        de: &mut D,
    ) -> Result<ResponseHeader, D::Error> {
        Ok(ResponseHeader {
            correlation_id: de.read_int32()?,
        })
    }
}
