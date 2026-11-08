use anyhow::{Context, Result, bail};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use crate::entity_ids::{stable_entity_id, stable_entity_id_u64, stable_host_id};
use crate::manifest::{self, Manifest};
use crate::output;
use crate::parser::{self, ParseOutcome};
use crate::schema::{EdgeType, LABEL_UNKNOWN, NodeType, NormalizedEdge, NormalizedNode};
use crate::{DEFAULT_SPLIT_POLICY, SCHEMA_VERSION};

#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub namespace: String,
    pub dataset_name: String,
    pub dataset_version: String,
    pub created_at_utc: Option<String>,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            namespace: "faithgraph-toy".to_string(),
            dataset_name: "unselected".to_string(),
            dataset_version: "unverified".to_string(),
            created_at_utc: None,
        }
    }
}

#[derive(Debug)]
pub struct ConversionResult {
    pub manifest: Manifest,
    pub nodes_path: PathBuf,
    pub edges_path: PathBuf,
    pub manifest_path: PathBuf,
}

pub fn convert(
    input: &Path,
    output_dir: &Path,
    options: &ConvertOptions,
) -> Result<ConversionResult> {
    let file = File::open(input)
        .with_context(|| format!("failed to open input file {}", input.display()))?;

    // Sorted by node_id so Parquet output does not depend on input order.
    let mut nodes: BTreeMap<u64, NormalizedNode> = BTreeMap::new();
    let mut edges: Vec<NormalizedEdge> = Vec::new();
    let mut seen_events: HashSet<(String, String)> = HashSet::new();
    let mut edge_lines: HashMap<u64, u64> = HashMap::new();

    let mut invalid_event_count: u64 = 0;
    let mut duplicate_event_count: u64 = 0;
    let mut minimum_timestamp_ns: Option<i64> = None;
    let mut maximum_timestamp_ns: Option<i64> = None;

    for parsed in parser::iter_events(BufReader::new(file)) {
        let event = match parsed.outcome {
            ParseOutcome::Event(event) => event,
            ParseOutcome::Skipped(reason) => {
                invalid_event_count += 1;
                eprintln!("line {}: {reason}", parsed.line_number);
                continue;
            }
        };

        if let Some(problem) = parser::validate(&event) {
            invalid_event_count += 1;
            eprintln!("line {}: {problem}", parsed.line_number);
            continue;
        }

        let Some(edge_type) = EdgeType::parse(&event.event_type) else {
            invalid_event_count += 1;
            eprintln!(
                "line {}: unknown event_type `{}`",
                parsed.line_number, event.event_type
            );
            continue;
        };

        let Some(subject_type) = NodeType::parse(&event.subject_type) else {
            invalid_event_count += 1;
            eprintln!(
                "line {}: unknown subject_type `{}`",
                parsed.line_number, event.subject_type
            );
            continue;
        };

        let Some(object_type) = NodeType::parse(&event.object_type) else {
            invalid_event_count += 1;
            eprintln!(
                "line {}: unknown object_type `{}`",
                parsed.line_number, event.object_type
            );
            continue;
        };

        if !seen_events.insert((event.host.clone(), event.event_id.clone())) {
            duplicate_event_count += 1;
            eprintln!(
                "line {}: duplicate event_id `{}` for host `{}`",
                parsed.line_number, event.event_id, event.host
            );
            continue;
        }

        let host_id = stable_host_id(&options.namespace, &event.host);

        let (subject_node_id, subject_hash) = stable_entity_id_pair(
            &options.namespace,
            &event.host,
            subject_type.as_str(),
            &event.subject_id,
        );
        let (object_node_id, object_hash) = stable_entity_id_pair(
            &options.namespace,
            &event.host,
            object_type.as_str(),
            &event.object_id,
        );

        upsert_node(
            &mut nodes,
            subject_node_id,
            subject_hash,
            host_id,
            subject_type,
            event.timestamp_ns,
        )?;
        upsert_node(
            &mut nodes,
            object_node_id,
            object_hash,
            host_id,
            object_type,
            event.timestamp_ns,
        )?;

        let edge_id =
            stable_entity_id_u64(&options.namespace, &event.host, "EVENT", &event.event_id);

        if let Some(previous_line) = edge_lines.insert(edge_id, parsed.line_number) {
            bail!(
                "edge id collision {edge_id:016x} between lines {previous_line} and {}",
                parsed.line_number
            );
        }

        edges.push(NormalizedEdge {
            edge_id,
            src_id: subject_node_id,
            dst_id: object_node_id,
            edge_type,
            timestamp_ns: event.timestamp_ns,
            host_id,
            event_sequence: parsed.line_number,
        });

        minimum_timestamp_ns = Some(minimum_timestamp_ns.map_or(event.timestamp_ns, |current| {
            current.min(event.timestamp_ns)
        }));
        maximum_timestamp_ns = Some(maximum_timestamp_ns.map_or(event.timestamp_ns, |current| {
            current.max(event.timestamp_ns)
        }));
    }

    edges.sort_by_key(|edge| (edge.timestamp_ns, edge.event_sequence, edge.edge_id));

    let nodes: Vec<NormalizedNode> = nodes.into_values().collect();

    let process_count = nodes
        .iter()
        .filter(|node| node.node_type == NodeType::Process)
        .count() as u64;
    let unknown_label_count = nodes
        .iter()
        .filter(|node| node.label == LABEL_UNKNOWN)
        .count() as u64;

    let manifest = Manifest {
        project: "FaithGraph".to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        dataset_name: options.dataset_name.clone(),
        dataset_version: options.dataset_version.clone(),
        raw_dataset_hash: manifest::file_hash(input)?,
        parser_version: env!("CARGO_PKG_VERSION").to_string(),
        configuration_hash: manifest::configuration_hash(&[
            ("dataset_name", options.dataset_name.as_str()),
            ("dataset_version", options.dataset_version.as_str()),
            ("namespace", options.namespace.as_str()),
            ("schema_version", SCHEMA_VERSION),
            ("split_policy", DEFAULT_SPLIT_POLICY),
        ]),
        created_at_utc: manifest::created_at_utc(options.created_at_utc.as_deref()),
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        process_count,
        malicious_process_count: 0,
        unknown_label_count,
        minimum_timestamp_ns: minimum_timestamp_ns.unwrap_or(0),
        maximum_timestamp_ns: maximum_timestamp_ns.unwrap_or(0),
        invalid_event_count,
        duplicate_event_count,
        split_policy: DEFAULT_SPLIT_POLICY.to_string(),
    };

    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let nodes_path = output_dir.join("nodes.parquet");
    let edges_path = output_dir.join("edges.parquet");
    let manifest_path = output_dir.join("manifest.json");

    output::write_nodes(&nodes_path, &nodes)?;
    output::write_edges(&edges_path, &edges)?;
    manifest.write(&manifest_path)?;

    Ok(ConversionResult {
        manifest,
        nodes_path,
        edges_path,
        manifest_path,
    })
}

fn stable_entity_id_pair(
    namespace: &str,
    host: &str,
    entity_type: &str,
    external_id: &str,
) -> (u64, String) {
    (
        stable_entity_id_u64(namespace, host, entity_type, external_id),
        stable_entity_id(namespace, host, entity_type, external_id),
    )
}

fn upsert_node(
    nodes: &mut BTreeMap<u64, NormalizedNode>,
    node_id: u64,
    external_id_hash: String,
    host_id: u32,
    node_type: NodeType,
    timestamp_ns: i64,
) -> Result<()> {
    match nodes.get_mut(&node_id) {
        Some(existing) => {
            if existing.external_id_hash != external_id_hash {
                bail!("node id collision {node_id:016x} maps to two distinct external identifiers");
            }

            existing.first_seen_ns = existing.first_seen_ns.min(timestamp_ns);
            existing.last_seen_ns = existing.last_seen_ns.max(timestamp_ns);
        }
        None => {
            nodes.insert(
                node_id,
                NormalizedNode {
                    node_id,
                    host_id,
                    node_type,
                    external_id_hash,
                    first_seen_ns: timestamp_ns,
                    last_seen_ns: timestamp_ns,
                    label: LABEL_UNKNOWN,
                },
            );
        }
    }

    Ok(())
}
