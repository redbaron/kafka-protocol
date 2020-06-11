use linked_hash_map::LinkedHashMap;

use crate::{Bytes, GroupId, Request};

type ProtocolName = String;

pub struct JoinGroupRequest /* p.name */ {
    pub group_id: GroupId,       // f.name: f.entityType
    pub session_timeout_ms: i32, // f.name: f.type

    //TODO: versions: 1+, ignorable, default=-1
    pub rebalance_timeout_ms: i32,
    pub member_id: String,

    //TODO: versions: 5+, nullableVersions: 5+
    pub group_instance_id: Option<String>, // f.name: nullable=true

    pub protocol_type: String,

    // f.type.startsWith("[]") + f.fields.contains(o: o.mapKey)
    // key: string
    pub protocols: LinkedHashMap<ProtocolName, JoinGroupRequestProtocol>,
}

impl crate::KafkaProtoMessage for JoinGroupRequest {
    fn serialize<S: crate::KafkaProtoEncoder>(
        &self,
        ver: i16,
        s: &mut S,
    ) -> std::result::Result<S::Ok, S::Error> {
        s.emit_string(&self.group_id)?;
        s.emit_int32(self.session_timeout_ms)?;

        if ver >= 1 {
            s.emit_int32(self.rebalance_timeout_ms)?;
        };

        s.emit_string(&self.member_id)?;

        if ver >= 5 {
            s.emit_nullable_string(self.group_instance_id.as_deref())?;
        };

        s.emit_string(&self.protocol_type)?;

        s.emit_array(self.protocols.values())
    }
}

pub struct JoinGroupRequestProtocol {
    pub name: ProtocolName,
    pub metadata: Bytes,
}

impl crate::KafkaProtoEncodable for JoinGroupRequestProtocol {
    fn emit<S: crate::KafkaProtoEncoder>(&self, s: &mut S) -> std::result::Result<S::Ok, S::Error> {
        s.emit_string(&self.name)?;
        s.emit_bytes(&self.metadata)
    }
}

impl Request for JoinGroupRequest {
    const API_KEY: i16 = 11; // j.apiKey

    const MIN_API_VERSION: i16 = 0; // j.validVersions.split("-")[0]
    const MAX_API_VERSION: i16 = 7; // j.validVersions.split("-")[1]
    const FLEXIBLE_VERSION: i16 = 6; // j.flexibleVersions[0]

    //type Response = JoinGroupResponse;
    type Response = ();
}
