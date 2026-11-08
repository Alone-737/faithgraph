pub mod convert;
pub mod entity_ids;
pub mod manifest;
pub mod output;
pub mod parser;
pub mod schema;

pub const SCHEMA_VERSION: &str = "0.1.0";

pub const DEFAULT_SPLIT_POLICY: &str = "campaign_aware_temporal";
