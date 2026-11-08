use arrow::array::{Int8Array, Int64Array, UInt8Array, UInt64Array};
use faithgraph_ingest::convert::{ConvertOptions, convert};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::{Path, PathBuf};

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/toy_events.jsonl")
}

fn options() -> ConvertOptions {
    ConvertOptions {
        namespace: "faithgraph-toy".to_string(),
        dataset_name: "toy".to_string(),
        dataset_version: "test".to_string(),
        created_at_utc: Some("2026-01-01T00:00:00Z".to_string()),
    }
}

fn read_parquet(path: &Path) -> Vec<arrow::record_batch::RecordBatch> {
    let file = File::open(path).expect("open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
    builder
        .build()
        .expect("parquet batch reader")
        .map(|batch| batch.expect("parquet batch"))
        .collect()
}

fn column_u8(batches: &[arrow::record_batch::RecordBatch], index: usize) -> Vec<u8> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("uint8 column")
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn converts_toy_fixture_end_to_end() {
    let temp = tempfile::tempdir().expect("temp dir");

    let result = convert(&fixture_path(), temp.path(), &options()).expect("conversion");
    let manifest = &result.manifest;

    assert_eq!(manifest.project, "FaithGraph");
    assert_eq!(manifest.schema_version, "0.1.0");
    assert_eq!(manifest.node_count, 7);
    assert_eq!(manifest.edge_count, 5);
    assert_eq!(manifest.process_count, 3);
    assert_eq!(manifest.malicious_process_count, 0);
    assert_eq!(manifest.unknown_label_count, 7);
    assert_eq!(manifest.invalid_event_count, 0);
    assert_eq!(manifest.duplicate_event_count, 0);
    assert_eq!(manifest.minimum_timestamp_ns, 1_000_000_000);
    assert_eq!(manifest.maximum_timestamp_ns, 1_400_000_000);

    assert!(result.nodes_path.exists());
    assert!(result.edges_path.exists());
    assert!(result.manifest_path.exists());

    let nodes = read_parquet(&result.nodes_path);
    let node_rows: usize = nodes
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(node_rows, 7);

    let node_types = column_u8(&nodes, 2);
    assert_eq!(node_types.iter().filter(|value| **value == 0).count(), 3);
    assert_eq!(node_types.iter().filter(|value| **value == 1).count(), 3);
    assert_eq!(node_types.iter().filter(|value| **value == 2).count(), 1);

    let labels: Vec<i8> = nodes
        .iter()
        .flat_map(|batch| {
            batch
                .column(6)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("int8 column")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(labels, vec![-1; 7]);

    let edges = read_parquet(&result.edges_path);
    let edge_rows: usize = edges
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(edge_rows, 5);

    assert_eq!(column_u8(&edges, 3), vec![0, 1, 2, 4, 1]);

    let timestamps: Vec<i64> = edges
        .iter()
        .flat_map(|batch| {
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        timestamps,
        vec![
            1_000_000_000,
            1_100_000_000,
            1_200_000_000,
            1_300_000_000,
            1_400_000_000
        ]
    );
}

#[test]
fn conversion_is_deterministic() {
    let first_dir = tempfile::tempdir().expect("temp dir");
    let second_dir = tempfile::tempdir().expect("temp dir");

    let first = convert(&fixture_path(), first_dir.path(), &options()).expect("first conversion");
    let second =
        convert(&fixture_path(), second_dir.path(), &options()).expect("second conversion");

    assert_eq!(first.manifest, second.manifest);

    assert_eq!(
        std::fs::read(&first.nodes_path).expect("first nodes"),
        std::fs::read(&second.nodes_path).expect("second nodes"),
        "node output must be byte-identical across runs"
    );
    assert_eq!(
        std::fs::read(&first.edges_path).expect("first edges"),
        std::fs::read(&second.edges_path).expect("second edges"),
        "edge output must be byte-identical across runs"
    );
}

#[test]
fn reports_invalid_and_duplicate_events() {
    let temp = tempfile::tempdir().expect("temp dir");
    let input = temp.path().join("events.jsonl");

    let valid = r#"{"event_id":"e1","timestamp_ns":1000000000,"host":"host-a","event_type":"SPAWNED","subject_type":"PROCESS","subject_id":"p100","object_type":"PROCESS","object_id":"p101"}"#;
    let unknown_type = r#"{"event_id":"e2","timestamp_ns":1100000000,"host":"host-a","event_type":"OPENED","subject_type":"PROCESS","subject_id":"p101","object_type":"FILE","object_id":"/tmp/x"}"#;
    let contents = format!("{valid}\n{valid}\nnot json\n\n{unknown_type}\n");

    std::fs::write(&input, contents).expect("write fixture");

    let output = temp.path().join("processed");
    let result = convert(&input, &output, &options()).expect("conversion");
    let manifest = &result.manifest;

    assert_eq!(manifest.node_count, 2);
    assert_eq!(manifest.edge_count, 1);
    assert_eq!(manifest.invalid_event_count, 3);
    assert_eq!(manifest.duplicate_event_count, 1);
}

#[test]
fn node_time_bounds_cover_all_events() {
    let temp = tempfile::tempdir().expect("temp dir");
    let result = convert(&fixture_path(), temp.path(), &options()).expect("conversion");

    let nodes = read_parquet(&result.nodes_path);
    let ids: Vec<u64> = nodes
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 column")
                .values()
                .to_vec()
        })
        .collect();

    let first_seen: Vec<i64> = nodes
        .iter()
        .flat_map(|batch| {
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
                .values()
                .to_vec()
        })
        .collect();

    let last_seen: Vec<i64> = nodes
        .iter()
        .flat_map(|batch| {
            batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
                .values()
                .to_vec()
        })
        .collect();

    assert_eq!(ids.len(), 7);
    for (first, last) in first_seen.iter().zip(last_seen.iter()) {
        assert!(first <= last);
        assert!(*first >= 1_000_000_000);
        assert!(*last <= 1_400_000_000);
    }

    assert!(last_seen.iter().all(|value| *value <= 1_400_000_000));
}
