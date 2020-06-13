pub mod error;
pub mod messages;
pub mod ser;

// TODO: what  is entityType? It looks like newtype to me
// See: EntityType.java, FieldSpec.java
// Following types are present:
//
// UNKNOWN(null),
// TRANSACTIONAL_ID(FieldType.StringFieldType.INSTANCE),
// PRODUCER_ID(FieldType.Int64FieldType.INSTANCE),
// GROUP_ID(FieldType.StringFieldType.INSTANCE),
// TOPIC_NAME(FieldType.StringFieldType.INSTANCE),
// BROKER_ID(FieldType.Int32FieldType.INSTANCE);

pub type TransactionalId = String;
pub type ProducerId = i64;
pub type GroupId = String;
pub type TopicName = String;
pub type BrokerId = i32;

type Bytes = Vec<u8>;

pub trait Request {
    const API_KEY: i16;

    const MIN_API_VERSION: i16;
    const MAX_API_VERSION: i16;
    const FLEXIBLE_VERSION: i16;

    type Response;
}
