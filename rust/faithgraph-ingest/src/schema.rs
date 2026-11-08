#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeType {
    Process = 0,
    File = 1,
    Endpoint = 2,
}

impl NodeType {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "PROCESS" => Some(Self::Process),
            "FILE" => Some(Self::File),
            "ENDPOINT" => Some(Self::Endpoint),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Process => "PROCESS",
            Self::File => "FILE",
            Self::Endpoint => "ENDPOINT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EdgeType {
    Spawned = 0,
    Read = 1,
    Wrote = 2,
    Executed = 3,
    ConnectedTo = 4,
}

impl EdgeType {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "SPAWNED" => Some(Self::Spawned),
            "READ" => Some(Self::Read),
            "WROTE" => Some(Self::Wrote),
            "EXECUTED" => Some(Self::Executed),
            "CONNECTED_TO" => Some(Self::ConnectedTo),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spawned => "SPAWNED",
            Self::Read => "READ",
            Self::Wrote => "WROTE",
            Self::Executed => "EXECUTED",
            Self::ConnectedTo => "CONNECTED_TO",
        }
    }
}

pub const LABEL_UNKNOWN: i8 = -1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedNode {
    pub node_id: u64,
    pub host_id: u32,
    pub node_type: NodeType,
    pub external_id_hash: String,
    pub first_seen_ns: i64,
    pub last_seen_ns: i64,
    pub label: i8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizedEdge {
    pub edge_id: u64,
    pub src_id: u64,
    pub dst_id: u64,
    pub edge_type: EdgeType,
    pub timestamp_ns: i64,
    pub host_id: u32,
    pub event_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::{EdgeType, NodeType};

    #[test]
    fn node_types_round_trip() {
        for node_type in [NodeType::Process, NodeType::File, NodeType::Endpoint] {
            assert_eq!(NodeType::parse(node_type.as_str()), Some(node_type));
        }
        assert_eq!(NodeType::Process.as_u8(), 0);
        assert_eq!(NodeType::File.as_u8(), 1);
        assert_eq!(NodeType::Endpoint.as_u8(), 2);
    }

    #[test]
    fn edge_types_round_trip() {
        for edge_type in [
            EdgeType::Spawned,
            EdgeType::Read,
            EdgeType::Wrote,
            EdgeType::Executed,
            EdgeType::ConnectedTo,
        ] {
            assert_eq!(EdgeType::parse(edge_type.as_str()), Some(edge_type));
        }
        assert_eq!(EdgeType::Spawned.as_u8(), 0);
        assert_eq!(EdgeType::ConnectedTo.as_u8(), 4);
    }

    #[test]
    fn unknown_types_are_rejected() {
        assert_eq!(NodeType::parse("SOCKET"), None);
        assert_eq!(EdgeType::parse("opened"), None);
    }
}
