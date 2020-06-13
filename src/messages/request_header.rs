use crate::ser::KafkaProtoEncoder;

pub struct RequestHeader<'a> {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}

impl RequestHeader<'_> {
    pub fn serialize<S: KafkaProtoEncoder>(&self, ver: i16, s: &mut S) -> Result<S::Ok, S::Error> {
        s.emit_int16(self.request_api_key)?;
        s.emit_int16(self.request_api_version)?;
        let mut r = s.emit_int32(self.correlation_id)?;

        if ver > 0 {
            r = s.emit_string(&self.client_id)?;
        }
        Ok(r)
    }
}
