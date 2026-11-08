use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use faithgraph_ingest::{
    convert::{self, ConvertOptions},
    entity_ids::stable_entity_id,
    parser::{self, ParseOutcome},
};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "faithgraph-ingest",
    version,
    about = "Validate, inspect, and convert FaithGraph provenance events"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Inspect {
        #[arg(short, long)]
        input: PathBuf,

        #[arg(long, default_value = "faithgraph-toy")]
        namespace: String,
    },

    Convert {
        #[arg(short, long)]
        input: PathBuf,

        #[arg(short, long)]
        output: PathBuf,

        #[arg(long, default_value = "faithgraph-toy")]
        namespace: String,

        #[arg(long, default_value = "unselected")]
        dataset_name: String,

        #[arg(long, default_value = "unverified")]
        dataset_version: String,

        #[arg(long)]
        created_at_utc: Option<String>,
    },
}

#[derive(Debug, Default)]
struct InspectionSummary {
    total_lines: u64,
    valid_events: u64,
    invalid_events: u64,
    minimum_timestamp_ns: Option<i64>,
    maximum_timestamp_ns: Option<i64>,
}

fn inspect_file(path: &Path, namespace: &str) -> Result<InspectionSummary> {
    let file = File::open(path)
        .with_context(|| format!("failed to open input file {}", path.display()))?;

    let mut summary = InspectionSummary::default();

    for parsed in parser::iter_events(BufReader::new(file)) {
        summary.total_lines += 1;

        let event = match parsed.outcome {
            ParseOutcome::Event(event) => event,
            ParseOutcome::Skipped(reason) => {
                summary.invalid_events += 1;
                eprintln!("line {}: {reason}", parsed.line_number);
                continue;
            }
        };

        if let Some(problem) = parser::validate(&event) {
            summary.invalid_events += 1;
            eprintln!("line {}: {problem}", parsed.line_number);
            continue;
        }

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

        Command::Convert {
            input,
            output,
            namespace,
            dataset_name,
            dataset_version,
            created_at_utc,
        } => {
            let options = ConvertOptions {
                namespace,
                dataset_name,
                dataset_version,
                created_at_utc,
            };

            let result = convert::convert(&input, &output, &options)?;
            let manifest = &result.manifest;

            println!();
            println!("Conversion summary");
            println!("------------------");
            println!("Nodes:              {}", manifest.node_count);
            println!("Edges:              {}", manifest.edge_count);
            println!("Processes:          {}", manifest.process_count);
            println!("Invalid events:     {}", manifest.invalid_event_count);
            println!("Duplicate events:   {}", manifest.duplicate_event_count);
            println!(
                "Timestamp range:    {} .. {}",
                manifest.minimum_timestamp_ns, manifest.maximum_timestamp_ns
            );
            println!("Nodes written:      {}", result.nodes_path.display());
            println!("Edges written:      {}", result.edges_path.display());
            println!("Manifest written:   {}", result.manifest_path.display());
        }
    }

    Ok(())
}
