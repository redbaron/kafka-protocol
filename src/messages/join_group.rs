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

pub struct JoinGroupRequestProtocol {
    pub name: ProtocolName,
    pub metadata: Bytes,
}

impl Request for JoinGroupRequest {
    const API_KEY: u16 = 11; // j.apiKey

    const MIN_API_VERSION: i16 = 0; // j.validVersions.split("-")[0]
    const MAX_API_VERSION: i16 = 7; // j.validVersions.split("-")[1]
    const FLEXIBLE_VERSION: i16 = 6; // j.flexibleVersions[0]

    //type Response = JoinGroupResponse;
    type Response = ();
}
