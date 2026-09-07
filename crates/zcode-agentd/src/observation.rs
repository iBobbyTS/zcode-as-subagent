use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::Read,
    path::Path,
};

pub const OBSERVATION_SCHEMA: &str = "zas-observation/1.1";
pub const VERIFIED_RUNTIME_VERSION: &str = "3.11.2";
pub const VERIFIED_EVENT_TYPE: &str = "model.streaming";
pub const VERIFIED_DELTA_POINTER: &str = "/params/payload/delta";
const VERIFIED_RUNTIME_PATH: &str = "/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs";
const VERIFIED_RUNTIME_SHA256: &str =
    "e9f1868c0fdb863537ed910ee3828b9be96b8c2fd805473f63b439e1113266b8";
const MAX_IDENTITIES: usize = 65_536;
const MAX_TOOL_TYPES: usize = 64;
const MAX_RECENT_CALLS: usize = 5;
const MAX_REASONING_CHARS: usize = 200;
const MAX_REASONING_REDACTION_BYTES: usize = 64 * 1024;
const MAX_ID_BYTES: usize = 512;
const MAX_ARGUMENT_BYTES: usize = 4 * 1024;
// The serialized argument JSON is embedded as one JSON string. Quotes and
// backslashes can at most double this already-escaped prefix, leaving room for
// the projection key and object envelope below the 4 KiB public cap.
const MAX_ARGUMENT_PREVIEW_BYTES: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationSnapshot {
    pub snapshot_seq: u64,
    pub tools: Vec<ObservedTool>,
    pub reasoning: ObservedReasoning,
    pub coverage: ObservationCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedTool {
    pub tool_name: String,
    pub call_count: u64,
    pub recent_calls: Vec<ObservedCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedCall {
    pub seq: u64,
    pub tool_call_id: String,
    pub arguments: Map<String, Value>,
    pub arguments_truncated: bool,
    pub redacted_fields: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedReasoning {
    pub text: String,
    pub char_count: usize,
    pub truncated: bool,
    pub source: ReasoningSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningSource {
    pub status: String,
    pub runtime_version: String,
    pub event_type: String,
    pub delta_pointer: String,
}

impl ReasoningSource {
    pub fn verified() -> Self {
        Self {
            status: "VERIFIED_RUNTIME_PUBLIC".into(),
            runtime_version: VERIFIED_RUNTIME_VERSION.into(),
            event_type: VERIFIED_EVENT_TYPE.into(),
            delta_pointer: VERIFIED_DELTA_POINTER.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationCoverage {
    pub tool_history_complete: bool,
    pub reasoning_complete: bool,
    pub dropped_events: u64,
}

impl ObservationSnapshot {
    pub fn unavailable() -> Self {
        Self {
            snapshot_seq: 0,
            tools: Vec::new(),
            reasoning: ObservedReasoning {
                text: String::new(),
                char_count: 0,
                truncated: false,
                source: ReasoningSource::verified(),
            },
            coverage: ObservationCoverage {
                tool_history_complete: false,
                reasoning_complete: false,
                dropped_events: 0,
            },
        }
    }
}

#[derive(Default)]
struct ToolGroup {
    call_count: u64,
    latest_seq: u64,
    recent_calls: VecDeque<ObservedCall>,
}

pub struct ObservationState {
    snapshot_seq: u64,
    next_call_seq: u64,
    seen_source_events: HashSet<String>,
    seen_calls: HashMap<String, (String, u64)>,
    tools: HashMap<String, ToolGroup>,
    reasoning_raw: String,
    reasoning_text: String,
    reasoning_truncated: bool,
    reasoning_redaction_overflow: bool,
    tool_history_complete: bool,
    reasoning_complete: bool,
    dropped_events: u64,
}

impl Default for ObservationState {
    fn default() -> Self {
        Self {
            snapshot_seq: 0,
            next_call_seq: 0,
            seen_source_events: HashSet::new(),
            seen_calls: HashMap::new(),
            tools: HashMap::new(),
            reasoning_raw: String::new(),
            reasoning_text: String::new(),
            reasoning_truncated: false,
            reasoning_redaction_overflow: false,
            tool_history_complete: true,
            reasoning_complete: true,
            dropped_events: 0,
        }
    }
}

impl ObservationState {
    pub fn observe_message(
        &mut self,
        method: &str,
        params: &Value,
        redact_text: fn(&str) -> String,
    ) {
        if method != "session/event"
            || params.get("type").and_then(Value::as_str) != Some(VERIFIED_EVENT_TYPE)
        {
            return;
        }
        let payload = params.get("payload").unwrap_or(&Value::Null);
        let kind = payload.get("kind").and_then(Value::as_str);
        if !matches!(kind, Some("reasoning_delta" | "tool_call")) {
            return;
        }
        let Some(event_id) = valid_id(params.get("eventId")) else {
            self.drop_event(kind == Some("tool_call"));
            return;
        };
        if self.seen_source_events.contains(event_id) {
            return;
        }
        if self.seen_source_events.len() >= MAX_IDENTITIES {
            self.drop_event(kind == Some("tool_call"));
            return;
        }
        self.seen_source_events.insert(event_id.to_owned());
        match kind {
            Some("reasoning_delta") => {
                let Some(delta) = payload.get("delta").and_then(Value::as_str) else {
                    self.reasoning_complete = false;
                    self.dropped_events = self.dropped_events.saturating_add(1);
                    self.bump_snapshot();
                    return;
                };
                if self.reasoning_redaction_overflow
                    || self.reasoning_raw.len().saturating_add(delta.len())
                        > MAX_REASONING_REDACTION_BYTES
                {
                    self.reasoning_raw.clear();
                    self.reasoning_text.clear();
                    self.reasoning_truncated = true;
                    self.reasoning_redaction_overflow = true;
                    self.reasoning_complete = false;
                    self.dropped_events = self.dropped_events.saturating_add(1);
                    self.bump_snapshot();
                    return;
                }
                self.reasoning_raw.push_str(delta);
                let redacted = redact_text(&self.reasoning_raw);
                let redacted_chars = redacted.chars().count();
                self.reasoning_text = redacted
                    .chars()
                    .rev()
                    .take(MAX_REASONING_CHARS)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                self.reasoning_truncated |= redacted_chars > MAX_REASONING_CHARS;
                self.bump_snapshot();
            }
            Some("tool_call") => self.observe_tool(params, payload, redact_text),
            _ => unreachable!("kind was filtered above"),
        }
    }

    fn observe_tool(&mut self, params: &Value, payload: &Value, redact_text: fn(&str) -> String) {
        let Some(call_id) = valid_id(payload.get("toolCallId")) else {
            self.drop_event(true);
            return;
        };
        let Some(tool_name) = valid_id(payload.get("toolName")) else {
            self.drop_event(true);
            return;
        };
        let turn_id = valid_id(params.get("turnId")).unwrap_or("");
        let identity = format!("{turn_id}\0{call_id}");
        let (arguments, arguments_truncated, redacted_fields) =
            sanitize_arguments(payload.get("input"), redact_text);

        if let Some((original_name, seq)) = self.seen_calls.get(&identity).cloned() {
            if let Some(call) = self
                .tools
                .get_mut(&original_name)
                .and_then(|group| group.recent_calls.iter_mut().find(|call| call.seq == seq))
            {
                if call.arguments != arguments
                    || call.arguments_truncated != arguments_truncated
                    || call.redacted_fields != redacted_fields
                {
                    call.arguments = arguments;
                    call.arguments_truncated = arguments_truncated;
                    call.redacted_fields = redacted_fields;
                    self.bump_snapshot();
                }
            }
            return;
        }
        if self.seen_calls.len() >= MAX_IDENTITIES {
            self.drop_event(true);
            return;
        }
        if !self.tools.contains_key(tool_name) && self.tools.len() >= MAX_TOOL_TYPES {
            self.drop_event(true);
            return;
        }
        self.next_call_seq = self.next_call_seq.saturating_add(1);
        let seq = self.next_call_seq;
        self.seen_calls
            .insert(identity, (tool_name.to_owned(), seq));
        let group = self.tools.entry(tool_name.to_owned()).or_default();
        group.call_count = group.call_count.saturating_add(1);
        group.latest_seq = seq;
        group.recent_calls.push_front(ObservedCall {
            seq,
            tool_call_id: call_id.to_owned(),
            arguments,
            arguments_truncated,
            redacted_fields,
        });
        group.recent_calls.truncate(MAX_RECENT_CALLS);
        self.bump_snapshot();
    }

    fn drop_event(&mut self, tool: bool) {
        if tool {
            self.tool_history_complete = false;
        } else {
            self.reasoning_complete = false;
        }
        self.dropped_events = self.dropped_events.saturating_add(1);
        self.bump_snapshot();
    }

    pub fn observe_loss(&mut self) {
        self.tool_history_complete = false;
        self.reasoning_complete = false;
        self.dropped_events = self.dropped_events.saturating_add(1);
        self.bump_snapshot();
    }

    fn bump_snapshot(&mut self) {
        self.snapshot_seq = self.snapshot_seq.saturating_add(1);
    }

    pub fn snapshot(&self) -> ObservationSnapshot {
        let mut ranked = self.tools.iter().collect::<Vec<_>>();
        ranked.sort_by(|(left_name, left), (right_name, right)| {
            right
                .call_count
                .cmp(&left.call_count)
                .then_with(|| right.latest_seq.cmp(&left.latest_seq))
                .then_with(|| left_name.cmp(right_name))
        });
        let tools = ranked
            .into_iter()
            .take(3)
            .map(|(name, group)| ObservedTool {
                tool_name: name.clone(),
                call_count: group.call_count,
                recent_calls: group.recent_calls.iter().cloned().collect(),
            })
            .collect();
        let text = self.reasoning_text.clone();
        ObservationSnapshot {
            snapshot_seq: self.snapshot_seq,
            tools,
            reasoning: ObservedReasoning {
                char_count: text.chars().count(),
                text,
                truncated: self.reasoning_truncated,
                source: ReasoningSource::verified(),
            },
            coverage: ObservationCoverage {
                tool_history_complete: self.tool_history_complete,
                reasoning_complete: self.reasoning_complete,
                dropped_events: self.dropped_events,
            },
        }
    }
}

fn valid_id(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= MAX_ID_BYTES && !value.contains('\0'))
}

fn sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    matches!(
        normalized.as_str(),
        "token" | "secret" | "password" | "api_key" | "apikey" | "private_key" | "authorization"
    )
}

fn sanitize_value(value: &Value, redact_text: fn(&str) -> String, redacted: &mut u64) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter_map(|(key, value)| {
                    if key == "encrypted_content" {
                        *redacted = redacted.saturating_add(1);
                        None
                    } else if sensitive_key(key) {
                        *redacted = redacted.saturating_add(1);
                        Some((key.clone(), Value::String("[REDACTED]".into())))
                    } else {
                        Some((key.clone(), sanitize_value(value, redact_text, redacted)))
                    }
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| sanitize_value(value, redact_text, redacted))
                .collect(),
        ),
        Value::String(value) => {
            let sanitized = redact_text(value);
            if sanitized != *value {
                *redacted = redacted.saturating_add(1);
            }
            Value::String(sanitized)
        }
        other => other.clone(),
    }
}

fn sanitize_arguments(
    input: Option<&Value>,
    redact_text: fn(&str) -> String,
) -> (Map<String, Value>, bool, u64) {
    let mut redacted_fields = 0;
    let sanitized = input
        .and_then(Value::as_object)
        .map(|object| {
            sanitize_value(
                &Value::Object(object.clone()),
                redact_text,
                &mut redacted_fields,
            )
        })
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let serialized = serde_json::to_string(&sanitized).unwrap_or_default();
    if serialized.len() <= MAX_ARGUMENT_BYTES {
        return (sanitized, false, redacted_fields);
    }
    let mut preview_end = serialized.len().min(MAX_ARGUMENT_PREVIEW_BYTES);
    while !serialized.is_char_boundary(preview_end) {
        preview_end -= 1;
    }
    let mut projected = Map::new();
    projected.insert(
        "_truncated_preview".into(),
        Value::String(serialized[..preview_end].to_owned()),
    );
    if serde_json::to_vec(&projected).map_or(true, |encoded| encoded.len() > MAX_ARGUMENT_BYTES) {
        projected.clear();
    }
    (projected, true, redacted_fields)
}

pub fn runtime_source_verified(path: Option<&Path>) -> bool {
    let Some(path) = path else { return false };
    let Ok(canonical) = path.canonicalize() else {
        return false;
    };
    if canonical != Path::new(VERIFIED_RUNTIME_PATH) {
        return false;
    }
    let Ok(mut file) = File::open(&canonical) else {
        return false;
    };
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let Ok(read) = file.read(&mut buffer) else {
            return false;
        };
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    format!("{:x}", digest.finalize()) == VERIFIED_RUNTIME_SHA256
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redact(value: &str) -> String {
        value.replace("secret-value", "[REDACTED]")
    }

    fn production_redact(value: &str) -> String {
        crate::redact_observation_text(value)
    }

    fn event(id: &str, kind: &str, payload: Value) -> Value {
        serde_json::json!({
            "type": "model.streaming",
            "eventId": id,
            "turnId": "turn-1",
            "payload": { "kind": kind, "toolCallId": payload.get("toolCallId"), "toolName": payload.get("toolName"), "input": payload.get("input"), "delta": payload.get("delta") }
        })
    }

    #[test]
    fn reasoning_uses_exact_public_selector_unicode_tail_and_event_id_dedupe() {
        let mut state = ObservationState::default();
        let first = event(
            "1",
            "reasoning_delta",
            serde_json::json!({"delta": "中🙂".repeat(75)}),
        );
        let second = event(
            "2",
            "reasoning_delta",
            serde_json::json!({"delta": "乙".repeat(100)}),
        );
        state.observe_message("session/event", &first, redact);
        state.observe_message("session/event", &first, redact);
        state.observe_message("session/event", &second, redact);
        state.observe_message(
            "session/event",
            &event("3", "other", serde_json::json!({"delta":"NO"})),
            redact,
        );
        let snapshot = state.snapshot();
        assert_eq!(snapshot.reasoning.char_count, 200);
        assert_eq!(
            snapshot.reasoning.text,
            format!("{}{}", "中🙂".repeat(50), "乙".repeat(100))
        );
        assert!(snapshot.reasoning.truncated);
        assert_eq!(snapshot.snapshot_seq, 2);
    }

    #[test]
    fn reasoning_redacts_production_patterns_across_delta_boundaries() {
        let cases = [
            (vec!["api_key=", "abcdefgh tail"], vec!["abcdefgh"]),
            (
                vec!["Authorization: Bearer abc", "defgh tail"],
                vec!["abcdefgh"],
            ),
            (
                vec!["visit https://user:", "pass@example.test/path tail"],
                vec!["user:pass", "example.test/path"],
            ),
            (
                vec![
                    "-----BEGIN PRIVATE KEY-----\nabc",
                    "def\n-----END PRIVATE KEY----- tail",
                ],
                vec!["abcdef", "BEGIN PRIVATE KEY", "END PRIVATE KEY"],
            ),
        ];
        for (case, (chunks, forbidden)) in cases.into_iter().enumerate() {
            let mut state = ObservationState::default();
            for (index, chunk) in chunks.into_iter().enumerate() {
                state.observe_message(
                    "session/event",
                    &event(
                        &format!("{case}-{index}"),
                        "reasoning_delta",
                        serde_json::json!({"delta":chunk}),
                    ),
                    production_redact,
                );
            }
            let snapshot = state.snapshot();
            assert!(snapshot.reasoning.text.contains("[REDACTED]"));
            for secret in forbidden {
                assert!(
                    !snapshot.reasoning.text.contains(secret),
                    "case {case} leaked {secret}"
                );
            }
            assert!(snapshot.coverage.reasoning_complete);
        }
    }

    #[test]
    fn incomplete_private_key_is_hidden_from_every_intermediate_snapshot() {
        let mut state = ObservationState::default();
        let chunks = [
            "ordinary\n-----BEGIN PRIVATE KEY-----\n",
            &"SYNTHETIC_KEY_BODY".repeat(30),
            "\n-----END PRIVATE KEY----- tail",
        ];
        for (index, chunk) in chunks.into_iter().enumerate() {
            state.observe_message(
                "session/event",
                &event(
                    &index.to_string(),
                    "reasoning_delta",
                    serde_json::json!({"delta":chunk}),
                ),
                production_redact,
            );
            let snapshot = state.snapshot();
            assert!(snapshot.reasoning.text.contains("[REDACTED]"));
            assert!(!snapshot.reasoning.text.contains("SYNTHETIC_KEY_BODY"));
            assert!(snapshot.reasoning.char_count <= MAX_REASONING_CHARS);
        }
        assert!(state.snapshot().reasoning.text.ends_with("[REDACTED] tail"));
    }

    #[test]
    fn ordinary_reasoning_remains_visible_during_streaming() {
        let mut state = ObservationState::default();
        for (index, (chunk, expected)) in [
            ("ordinary ", "ordinary "),
            ("streaming text", "ordinary streaming text"),
        ]
        .into_iter()
        .enumerate()
        {
            state.observe_message(
                "session/event",
                &event(
                    &index.to_string(),
                    "reasoning_delta",
                    serde_json::json!({"delta":chunk}),
                ),
                production_redact,
            );
            assert_eq!(state.snapshot().reasoning.text, expected);
        }
    }

    #[test]
    fn reasoning_redacts_before_applying_the_unicode_tail() {
        let mut state = ObservationState::default();
        state.observe_message(
            "session/event",
            &event(
                "1",
                "reasoning_delta",
                serde_json::json!({"delta":format!("{} api_key=", "中🙂".repeat(130))}),
            ),
            production_redact,
        );
        state.observe_message(
            "session/event",
            &event(
                "2",
                "reasoning_delta",
                serde_json::json!({"delta":"abcdefgh tail"}),
            ),
            production_redact,
        );
        let snapshot = state.snapshot();
        assert_eq!(snapshot.reasoning.char_count, 200);
        assert!(snapshot.reasoning.truncated);
        assert!(snapshot.reasoning.text.ends_with("[REDACTED] tail"));
        assert!(!snapshot.reasoning.text.contains("abcdefgh"));
    }

    #[test]
    fn reasoning_redaction_budget_fails_closed_with_explicit_coverage_loss() {
        let mut state = ObservationState::default();
        state.observe_message(
            "session/event",
            &event(
                "1",
                "reasoning_delta",
                serde_json::json!({"delta":"x".repeat(MAX_REASONING_REDACTION_BYTES)}),
            ),
            production_redact,
        );
        state.observe_message(
            "session/event",
            &event(
                "2",
                "reasoning_delta",
                serde_json::json!({"delta":"api_key=must-not-leak"}),
            ),
            production_redact,
        );
        let snapshot = state.snapshot();
        assert_eq!(snapshot.reasoning.text, "");
        assert!(snapshot.reasoning.truncated);
        assert!(!snapshot.coverage.reasoning_complete);
        assert_eq!(snapshot.coverage.dropped_events, 1);
    }

    #[test]
    fn tools_rank_by_count_then_recency_and_keep_five_recent_calls() {
        let mut state = ObservationState::default();
        let mut id = 0;
        for (name, count) in [("Read", 8), ("Bash", 6), ("Search", 4), ("Edit", 1)] {
            for call in 0..count {
                id += 1;
                let payload = serde_json::json!({"toolCallId": format!("call-{id}"), "toolName": name, "input": {"n": call}});
                state.observe_message(
                    "session/event",
                    &event(&id.to_string(), "tool_call", payload),
                    redact,
                );
            }
        }
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot
                .tools
                .iter()
                .map(|tool| tool.tool_name.as_str())
                .collect::<Vec<_>>(),
            ["Read", "Bash", "Search"]
        );
        assert_eq!(snapshot.tools[0].call_count, 8);
        assert_eq!(snapshot.tools[0].recent_calls.len(), 5);
        assert_eq!(
            snapshot.tools[0]
                .recent_calls
                .iter()
                .map(|call| call.seq)
                .collect::<Vec<_>>(),
            [8, 7, 6, 5, 4]
        );
    }

    #[test]
    fn call_updates_do_not_recount_and_arguments_exclude_sensitive_content() {
        let mut state = ObservationState::default();
        let initial = serde_json::json!({"toolCallId":"call", "toolName":"Bash", "input": {}});
        let update = serde_json::json!({"toolCallId":"call", "toolName":"Bash", "input": {"command":"echo secret-value", "nested":{"encrypted_content":"NEVER"}, "token":"NEVER"}});
        state.observe_message("session/event", &event("1", "tool_call", initial), redact);
        state.observe_message("session/event", &event("2", "tool_call", update), redact);
        let snapshot = state.snapshot();
        let tool = &snapshot.tools[0];
        assert_eq!(tool.call_count, 1);
        assert_eq!(tool.recent_calls[0].seq, 1);
        let encoded = serde_json::to_string(&tool.recent_calls[0].arguments).unwrap();
        assert!(!encoded.contains("NEVER"));
        assert!(!encoded.contains("encrypted_content"));
        assert!(encoded.contains("[REDACTED]"));
        assert!(tool.recent_calls[0].redacted_fields >= 3);
    }

    #[test]
    fn identical_arguments_with_distinct_call_ids_are_counted() {
        let mut state = ObservationState::default();
        for id in 1..=3 {
            let payload = serde_json::json!({"toolCallId":id.to_string(), "toolName":"Bash", "input":{"command":"true"}});
            state.observe_message(
                "session/event",
                &event(&id.to_string(), "tool_call", payload),
                redact,
            );
        }
        assert_eq!(state.snapshot().tools[0].call_count, 3);
    }

    #[test]
    fn long_arguments_are_bounded_and_marked_truncated() {
        let mut state = ObservationState::default();
        let payload = serde_json::json!({"toolCallId":"1", "toolName":"Read", "input":{"path":"x".repeat(10_000)}});
        state.observe_message("session/event", &event("1", "tool_call", payload), redact);
        let call = &state.snapshot().tools[0].recent_calls[0];
        assert!(call.arguments_truncated);
        assert!(serde_json::to_vec(&call.arguments).unwrap().len() <= MAX_ARGUMENT_BYTES);
    }

    #[test]
    fn near_driver_limit_arguments_use_bounded_linear_projection_work() {
        for size in [256 * 1024, 900 * 1024] {
            let mut state = ObservationState::default();
            let payload = serde_json::json!({
                "toolCallId":"1",
                "toolName":"Bash",
                "input":{"command":"x".repeat(size), "token":"must-not-copy"}
            });
            let started = std::time::Instant::now();
            state.observe_message(
                "session/event",
                &event("1", "tool_call", payload),
                production_redact,
            );
            let elapsed = started.elapsed();
            let snapshot = state.snapshot();
            let call = &snapshot.tools[0].recent_calls[0];
            assert!(call.arguments_truncated);
            assert!(serde_json::to_vec(&call.arguments).unwrap().len() <= MAX_ARGUMENT_BYTES);
            assert!(!serde_json::to_string(&call.arguments)
                .unwrap()
                .contains("must-not-copy"));
            assert!(
                elapsed < std::time::Duration::from_secs(5),
                "{size} bytes took {elapsed:?}"
            );
        }
    }

    #[test]
    fn verified_runtime_fixture_uses_only_the_confirmed_public_fields() {
        let events: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/live-agent/non-git-based/observation-public-fixture.json"
        ))
        .unwrap();
        let expected_text = events
            .iter()
            .filter_map(|event| {
                event["params"]["payload"]["delta"]
                    .as_str()
                    .filter(|_| event["params"]["payload"]["kind"] == "reasoning_delta")
            })
            .collect::<String>();
        let mut state = ObservationState::default();
        for event in &events {
            state.observe_message(event["method"].as_str().unwrap(), &event["params"], redact);
        }
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.reasoning.text,
            expected_text
                .chars()
                .rev()
                .take(200)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
        );
        assert_eq!(snapshot.reasoning.char_count, 200);
        assert!(snapshot.reasoning.truncated);
        assert_eq!(snapshot.tools.len(), 2);
        assert_eq!(
            snapshot
                .tools
                .iter()
                .map(|tool| tool.tool_name.as_str())
                .collect::<Vec<_>>(),
            ["Bash", "Read"]
        );
        assert!(snapshot.coverage.tool_history_complete);
        assert!(snapshot.coverage.reasoning_complete);
    }

    #[test]
    fn malformed_public_candidates_create_honest_coverage_gaps() {
        let mut state = ObservationState::default();
        state.observe_message(
            "session/event",
            &serde_json::json!({"type":"model.streaming", "eventId":"1", "payload":{"kind":"reasoning_delta", "delta":42}}),
            redact,
        );
        state.observe_message(
            "session/event",
            &serde_json::json!({"type":"model.streaming", "eventId":"2", "payload":{"kind":"tool_call", "toolName":"Read", "input":{}}}),
            redact,
        );
        let snapshot = state.snapshot();
        assert!(!snapshot.coverage.reasoning_complete);
        assert!(!snapshot.coverage.tool_history_complete);
        assert_eq!(snapshot.coverage.dropped_events, 2);
        assert_eq!(snapshot.snapshot_seq, 2);
    }

    #[test]
    fn ties_use_latest_call_then_name_and_empty_delta_is_valid() {
        let mut state = ObservationState::default();
        state.observe_message(
            "session/event",
            &event(
                "1",
                "tool_call",
                serde_json::json!({"toolCallId":"z", "toolName":"Zed", "input":{}}),
            ),
            redact,
        );
        state.observe_message(
            "session/event",
            &event(
                "2",
                "tool_call",
                serde_json::json!({"toolCallId":"a", "toolName":"Alpha", "input":{}}),
            ),
            redact,
        );
        state.observe_message(
            "session/event",
            &event("3", "reasoning_delta", serde_json::json!({"delta":""})),
            redact,
        );
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot
                .tools
                .iter()
                .map(|tool| tool.tool_name.as_str())
                .collect::<Vec<_>>(),
            ["Alpha", "Zed"]
        );
        assert_eq!(snapshot.reasoning.char_count, 0);
        assert!(snapshot.coverage.reasoning_complete);
        assert_eq!(snapshot.snapshot_seq, 3);
    }

    #[test]
    fn tool_type_and_identity_caps_stop_admission_and_report_loss() {
        let mut types = ObservationState::default();
        for id in 0..=MAX_TOOL_TYPES {
            types.observe_message(
                "session/event",
                &event(
                    &id.to_string(),
                    "tool_call",
                    serde_json::json!({"toolCallId":id.to_string(), "toolName":format!("tool-{id}"), "input":{}}),
                ),
                redact,
            );
        }
        assert_eq!(types.tools.len(), MAX_TOOL_TYPES);
        assert!(!types.snapshot().coverage.tool_history_complete);

        let mut identities = ObservationState::default();
        for id in 0..=MAX_IDENTITIES {
            identities.observe_message(
                "session/event",
                &event(
                    &id.to_string(),
                    "tool_call",
                    serde_json::json!({"toolCallId":id.to_string(), "toolName":"Read", "input":{}}),
                ),
                redact,
            );
        }
        let snapshot = identities.snapshot();
        assert_eq!(snapshot.tools[0].call_count, MAX_IDENTITIES as u64);
        assert!(!snapshot.coverage.tool_history_complete);
        assert_eq!(snapshot.coverage.dropped_events, 1);
    }

    #[test]
    fn source_verification_is_bound_to_the_pinned_path_and_hash() {
        let pinned = Path::new(VERIFIED_RUNTIME_PATH);
        if pinned.exists() {
            assert!(runtime_source_verified(Some(pinned)));
        }
        let other = tempfile::NamedTempFile::new().unwrap();
        assert!(!runtime_source_verified(Some(other.path())));
        assert!(!runtime_source_verified(None));
    }
}
