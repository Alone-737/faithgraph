use anyhow::{Context, Result};
use blake3::Hasher;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(
    name = "faithgraph-ingest",
    version,
    about = "Validate and inspect FaithGraph provenance events"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect a JSONL provenance-event file.
    Inspect {
        /// Input JSONL file.
        #[arg(short, long)]
        input: PathBuf,

        /// Dataset namespace used for deterministic entity IDs.
        #[arg(long, default_value = "faithgraph-toy")]
        namespace: String,
    },
}

#[derive(Debug, Deserialize)]
struct RawEvent {
    event_id: String,
    timestamp_ns: i64,
    host: String,
    event_type: String,
    subject_type: String,
    subject_id: String,
    object_type: String,
    object_id: String,
}

#[derive(Debug, Default)]
struct InspectionSummary {
    total_lines: u64,
    valid_events: u64,
    invalid_events: u64,
    minimum_timestamp_ns: Option<i64>,
    maximum_timestamp_ns: Option<i64>,
}

fn stable_entity_id(namespace: &str, host: &str, entity_type: &str, external_id: &str) -> String {
    let mut hasher = Hasher::new();

    for component in [namespace, host, entity_type, external_id] {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }

    hasher.finalize().to_hex().to_string()
}

fn inspect_file(path: &Path, namespace: &str) -> Result<InspectionSummary> {
    let file = File::open(path)
        .with_context(|| format!("failed to open input file {}", path.display()))?;

    let reader = BufReader::new(file);
    let mut summary = InspectionSummary::default();

    for (index, line_result) in reader.lines().enumerate() {
        summary.total_lines += 1;

        let line = match line_result {
            Ok(value) => value,
            Err(error) => {
                summary.invalid_events += 1;
                eprintln!("line {}: failed to read: {error}", index + 1);
                continue;
            }
        };

        if line.trim().is_empty() {
            summary.invalid_events += 1;
            eprintln!("line {}: empty record", index + 1);
            continue;
        }

        let event: RawEvent = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                summary.invalid_events += 1;
                eprintln!("line {}: invalid JSON event: {error}", index + 1);
                continue;
            }
        };

        summary.valid_events += 1;

        summary.minimum_timestamp_ns = Some(
            summary
                .minimum_timestamp_ns
                .map_or(event.timestamp_ns, |current| {
                    current.min(event.timestamp_ns)
                }),
        );

        summary.maximum_timestamp_ns = Some(
            summary
                .maximum_timestamp_ns
                .map_or(event.timestamp_ns, |current| {
                    current.max(event.timestamp_ns)
                }),
        );

        let subject_id = stable_entity_id(
            namespace,
            &event.host,
            &event.subject_type,
            &event.subject_id,
        );

        let object_id =
            stable_entity_id(namespace, &event.host, &event.object_type, &event.object_id);

        println!(
            "{} {} {} -> {} [{}]",
            event.event_id,
            event.timestamp_ns,
            &subject_id[..16],
            &object_id[..16],
            event.event_type
        );
    }

    Ok(summary)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Inspect { input, namespace } => {
            let summary = inspect_file(&input, &namespace)?;

            println!();
            println!("Inspection summary");
            println!("------------------");
            println!("Total lines:         {}", summary.total_lines);
            println!("Valid events:       {}", summary.valid_events);
            println!("Invalid events:     {}", summary.invalid_events);
            println!(
                "Minimum timestamp:  {}",
                summary
                    .minimum_timestamp_ns
                    .map_or_else(|| "none".to_string(), |value| value.to_string())
            );
            println!(
                "Maximum timestamp:  {}",
                summary
                    .maximum_timestamp_ns
                    .map_or_else(|| "none".to_string(), |value| value.to_string())
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::stable_entity_id;

    #[test]
    fn stable_id_is_deterministic() {
        let first = stable_entity_id("toy", "host-a", "PROCESS", "p101");
        let second = stable_entity_id("toy", "host-a", "PROCESS", "p101");

        assert_eq!(first, second);
    }

    #[test]
    fn stable_id_changes_when_entity_changes() {
        let first = stable_entity_id("toy", "host-a", "PROCESS", "p101");
        let second = stable_entity_id("toy", "host-a", "PROCESS", "p102");

        assert_ne!(first, second);
    }

    #[test]
    fn component_boundaries_do_not_collide() {
        let first = stable_entity_id("toy", "ab", "c", "d");
        let second = stable_entity_id("toy", "a", "bc", "d");

        assert_ne!(first, second);
    }
}
