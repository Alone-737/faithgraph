use serde::Deserialize;
use std::io::BufRead;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RawEvent {
    pub event_id: String,
    pub timestamp_ns: i64,
    pub host: String,
    pub event_type: String,
    pub subject_type: String,
    pub subject_id: String,
    pub object_type: String,
    pub object_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome {
    Event(RawEvent),
    Skipped(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLine {
    pub line_number: u64,
    pub outcome: ParseOutcome,
}

pub struct JsonlEvents<R> {
    reader: R,
    line_number: u64,
    buffer: String,
    finished: bool,
}

impl<R: BufRead> JsonlEvents<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line_number: 0,
            buffer: String::new(),
            finished: false,
        }
    }
}

impl<R: BufRead> Iterator for JsonlEvents<R> {
    type Item = ParsedLine;

    fn next(&mut self) -> Option<ParsedLine> {
        if self.finished {
            return None;
        }

        self.buffer.clear();
        let line_number = self.line_number + 1;

        match self.reader.read_line(&mut self.buffer) {
            Ok(0) => {
                self.finished = true;
                None
            }
            Ok(_) => {
                self.line_number = line_number;
                Some(ParsedLine {
                    line_number,
                    outcome: parse_line(&self.buffer),
                })
            }
            Err(error) => {
                self.line_number = line_number;
                self.finished = true;
                Some(ParsedLine {
                    line_number,
                    outcome: ParseOutcome::Skipped(format!("failed to read: {error}")),
                })
            }
        }
    }
}

pub fn iter_events<R: BufRead>(reader: R) -> JsonlEvents<R> {
    JsonlEvents::new(reader)
}

pub fn parse_line(line: &str) -> ParseOutcome {
    let trimmed = line.trim();

    if trimmed.is_empty() {
        return ParseOutcome::Skipped("empty record".to_string());
    }

    match serde_json::from_str::<RawEvent>(trimmed) {
        Ok(event) => ParseOutcome::Event(event),
        Err(error) => ParseOutcome::Skipped(format!("invalid JSON event: {error}")),
    }
}

pub fn validate(event: &RawEvent) -> Option<String> {
    let required = [
        ("event_id", event.event_id.as_str()),
        ("host", event.host.as_str()),
        ("event_type", event.event_type.as_str()),
        ("subject_type", event.subject_type.as_str()),
        ("subject_id", event.subject_id.as_str()),
        ("object_type", event.object_type.as_str()),
        ("object_id", event.object_id.as_str()),
    ];

    for (name, value) in required {
        if value.trim().is_empty() {
            return Some(format!("missing required field `{name}`"));
        }
    }

    if event.timestamp_ns < 0 {
        return Some(format!("negative timestamp_ns `{}`", event.timestamp_ns));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{JsonlEvents, ParseOutcome, parse_line, validate};
    use std::io::Cursor;

    const VALID: &str = r#"{"event_id":"e1","timestamp_ns":1000000000,"host":"host-a","event_type":"SPAWNED","subject_type":"PROCESS","subject_id":"p100","object_type":"PROCESS","object_id":"p101"}"#;

    #[test]
    fn parses_valid_event() {
        let ParseOutcome::Event(event) = parse_line(VALID) else {
            panic!("expected a parsed event");
        };

        assert_eq!(event.event_id, "e1");
        assert_eq!(event.timestamp_ns, 1_000_000_000);
        assert_eq!(event.event_type, "SPAWNED");
        assert_eq!(validate(&event), None);
    }

    #[test]
    fn rejects_blank_and_malformed_lines() {
        assert!(matches!(parse_line("   "), ParseOutcome::Skipped(_)));
        assert!(matches!(parse_line("{not json"), ParseOutcome::Skipped(_)));
    }

    #[test]
    fn rejects_missing_required_fields() {
        let line = r#"{"event_id":"e1","timestamp_ns":1,"host":"h","event_type":"READ","subject_type":"PROCESS","subject_id":"","object_type":"FILE","object_id":"/tmp/x"}"#;
        let ParseOutcome::Event(event) = parse_line(line) else {
            panic!("expected a parsed event");
        };

        assert!(validate(&event).is_some());
    }

    #[test]
    fn rejects_negative_timestamps() {
        let line = VALID.replace("1000000000", "-1");
        let ParseOutcome::Event(event) = parse_line(&line) else {
            panic!("expected a parsed event");
        };

        assert!(validate(&event).is_some());
    }

    #[test]
    fn iterator_reports_line_numbers() {
        let input = format!("{VALID}\n\nnot-json\n");
        let parsed: Vec<_> = JsonlEvents::new(Cursor::new(input)).collect();

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].line_number, 1);
        assert!(matches!(parsed[0].outcome, ParseOutcome::Event(_)));
        assert!(matches!(parsed[1].outcome, ParseOutcome::Skipped(_)));
        assert!(matches!(parsed[2].outcome, ParseOutcome::Skipped(_)));
    }

    #[test]
    fn iterator_handles_missing_trailing_newline() {
        let parsed: Vec<_> = JsonlEvents::new(Cursor::new(VALID.to_string())).collect();

        assert_eq!(parsed.len(), 1);
        assert!(matches!(parsed[0].outcome, ParseOutcome::Event(_)));
    }
}
