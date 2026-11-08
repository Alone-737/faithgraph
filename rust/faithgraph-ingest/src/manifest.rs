use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub project: String,
    pub schema_version: String,
    pub dataset_name: String,
    pub dataset_version: String,
    pub raw_dataset_hash: String,
    pub parser_version: String,
    pub configuration_hash: String,
    pub created_at_utc: String,
    pub node_count: u64,
    pub edge_count: u64,
    pub process_count: u64,
    pub malicious_process_count: u64,
    pub unknown_label_count: u64,
    pub minimum_timestamp_ns: i64,
    pub maximum_timestamp_ns: i64,
    pub invalid_event_count: u64,
    pub duplicate_event_count: u64,
    pub split_policy: String,
}

impl Manifest {
    pub fn to_json(&self) -> Result<String> {
        let mut json =
            serde_json::to_string_pretty(self).context("failed to serialize manifest")?;
        json.push('\n');
        Ok(json)
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        std::fs::write(path, self.to_json()?)
            .with_context(|| format!("failed to write manifest {}", path.display()))
    }
}

pub fn file_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("failed to open input file {} for hashing", path.display()))?;
    let mut hasher = blake3::Hasher::new();

    io::copy(&mut file, &mut hasher)
        .with_context(|| format!("failed to hash input file {}", path.display()))?;

    Ok(hasher.finalize().to_hex().to_string())
}

pub fn configuration_hash(parts: &[(&str, &str)]) -> String {
    let mut sorted = parts.to_vec();
    sorted.sort_by(|left, right| left.0.cmp(right.0));

    let mut hasher = blake3::Hasher::new();
    for (key, value) in sorted {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }

    hasher.finalize().to_hex().to_string()
}

pub fn created_at_utc(override_value: Option<&str>) -> String {
    // Explicit override wins; SOURCE_DATE_EPOCH keeps rebuilds reproducible.
    if let Some(value) = override_value {
        return value.to_string();
    }

    if let Ok(raw) = std::env::var("SOURCE_DATE_EPOCH")
        && let Ok(seconds) = raw.parse::<i64>()
        && let Some(timestamp) = DateTime::from_timestamp(seconds, 0)
    {
        return timestamp.to_rfc3339_opts(SecondsFormat::Secs, true);
    }

    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::{Manifest, configuration_hash, created_at_utc};

    fn sample_manifest() -> Manifest {
        Manifest {
            project: "FaithGraph".to_string(),
            schema_version: "0.1.0".to_string(),
            dataset_name: "toy".to_string(),
            dataset_version: "test".to_string(),
            raw_dataset_hash: "abc".to_string(),
            parser_version: "0.1.0".to_string(),
            configuration_hash: "def".to_string(),
            created_at_utc: "2026-01-01T00:00:00Z".to_string(),
            node_count: 7,
            edge_count: 5,
            process_count: 3,
            malicious_process_count: 0,
            unknown_label_count: 7,
            minimum_timestamp_ns: 1_000_000_000,
            maximum_timestamp_ns: 1_400_000_000,
            invalid_event_count: 0,
            duplicate_event_count: 0,
            split_policy: "campaign_aware_temporal".to_string(),
        }
    }

    #[test]
    fn manifest_json_has_trailing_newline_and_round_trips() {
        let manifest = sample_manifest();
        let json = manifest.to_json().expect("serialize");

        assert!(json.ends_with('\n'));
        assert!(json.contains("\"split_policy\": \"campaign_aware_temporal\""));

        let parsed: Manifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn configuration_hash_is_order_independent() {
        let first = configuration_hash(&[("a", "1"), ("b", "2")]);
        let second = configuration_hash(&[("b", "2"), ("a", "1")]);

        assert_eq!(first, second);
    }

    #[test]
    fn configuration_hash_separates_component_boundaries() {
        let first = configuration_hash(&[("a", "bc")]);
        let second = configuration_hash(&[("ab", "c")]);

        assert_ne!(first, second);
    }

    #[test]
    fn created_at_prefers_explicit_override() {
        assert_eq!(
            created_at_utc(Some("2026-01-01T00:00:00Z")),
            "2026-01-01T00:00:00Z"
        );
    }
}
