use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, Json, ServerHandler, ServiceExt,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;
use zcode_agent_preparation::{GeneralTaskManifest, PermissionMode, GENERAL_TASK_SCHEMA};
use zcode_agent_store::TaskOutcome;
use zcode_agentd::rpc::{
    AgentCapabilitiesView, CapabilityMaturityView, ComponentStateView, GeneralSubmitInput,
    MessageInput, RespondInput, ResponseDecision, ResponseOutcomeView, RpcClient, RpcMethod,
    RpcOutcome, RpcRequest, RpcSuccess, SubmissionDispositionView, SystemStatusView,
    TaskActivityStateView, TaskActivityView, TaskListQuery, TaskObservationView, TaskPhaseFilter,
    TaskPollQuery, TaskResultView, TaskView, TelemetryStatusView, RPC_VERSION,
};

use crate::{
    protocol_error, public_error, public_transport_error, PublicDecision, PublicPendingRequest,
    PublicResponseDisposition,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicPermissionMode {
    Build,
    Edit,
    Plan,
    Yolo,
}

impl Default for PublicPermissionMode {
    fn default() -> Self {
        Self::Build
    }
}

impl From<PublicPermissionMode> for PermissionMode {
    fn from(value: PublicPermissionMode) -> Self {
        match value {
            PublicPermissionMode::Build => Self::Build,
            PublicPermissionMode::Edit => Self::Edit,
            PublicPermissionMode::Plan => Self::Plan,
            PublicPermissionMode::Yolo => Self::Yolo,
        }
    }
}

pub const PUBLIC_TOOLS: [&str; 10] = [
    "zcode_subagent_cancel",
    "zcode_subagent_close",
    "zcode_subagent_list",
    "zcode_subagent_observe",
    "zcode_subagent_poll",
    "zcode_subagent_respond",
    "zcode_subagent_result",
    "zcode_subagent_send",
    "zcode_subagent_spawn",
    "zcode_subagent_status",
];

const MAX_ID_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 4096;
const MAX_PROMPT_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_REASON_BYTES: usize = 2048;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyInput {}

fn optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}
fn validate_text(value: &str, field: &str, max: usize) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        Err(format!("validation: {field} is invalid"))
    } else {
        Ok(())
    }
}

fn validate_path(value: &str, field: &str) -> Result<(), String> {
    validate_text(value, field, MAX_PATH_BYTES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicComponentState {
    Ready,
    Degraded,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicCapabilityMaturity {
    BetaReady,
    ExperimentalUnverifiedRuntime,
}

impl From<CapabilityMaturityView> for PublicCapabilityMaturity {
    fn from(value: CapabilityMaturityView) -> Self {
        match value {
            CapabilityMaturityView::BetaReady => Self::BetaReady,
            CapabilityMaturityView::ExperimentalUnverifiedRuntime => {
                Self::ExperimentalUnverifiedRuntime
            }
        }
    }
}

impl From<ComponentStateView> for PublicComponentState {
    fn from(value: ComponentStateView) -> Self {
        match value {
            ComponentStateView::Ready => Self::Ready,
            ComponentStateView::Degraded => Self::Degraded,
            ComponentStateView::Unavailable => Self::Unavailable,
            ComponentStateView::Unknown => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicAgentCapabilities {
    pub max_rpc_frame_bytes: usize,
    pub max_wait_ms: u64,
    pub maturity: BTreeMap<String, PublicCapabilityMaturity>,
    pub observation: PublicObservationCapability,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicObservationCapability {
    pub protocol: String,
    pub public_reasoning_default: bool,
    pub runtime_source_verified: bool,
    pub defaults: PublicObservationDefaults,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicObservationDefaults {
    pub top_tools: usize,
    pub recent_calls_per_tool: usize,
    pub reasoning_chars: usize,
}

impl From<AgentCapabilitiesView> for PublicAgentCapabilities {
    fn from(value: AgentCapabilitiesView) -> Self {
        let maturity = value
            .maturity
            .into_iter()
            .map(|(name, maturity)| (name, maturity.into()))
            .collect();
        Self {
            max_rpc_frame_bytes: value.max_rpc_frame_bytes,
            max_wait_ms: value.max_wait_ms,
            maturity,
            observation: PublicObservationCapability {
                protocol: value.observation.protocol,
                public_reasoning_default: value.observation.public_reasoning_default,
                runtime_source_verified: value.observation.runtime_source_verified,
                defaults: PublicObservationDefaults {
                    top_tools: value.observation.defaults.top_tools,
                    recent_calls_per_tool: value.observation.defaults.recent_calls_per_tool,
                    reasoning_chars: value.observation.defaults.reasoning_chars,
                },
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
pub enum PublicObservationSchema {
    #[serde(rename = "zas-observation/1.1")]
    Version1_1,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicObservationCountScope {
    AgentLifetime,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
pub enum PublicReasoningSourceStatus {
    #[serde(rename = "VERIFIED_RUNTIME_PUBLIC")]
    VerifiedRuntimePublic,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentObserveOutput {
    pub schema: PublicObservationSchema,
    pub agent_id: String,
    pub service_generation: String,
    pub snapshot_seq: u64,
    pub count_scope: PublicObservationCountScope,
    pub tools: Vec<PublicObservedTool>,
    pub reasoning: PublicObservedReasoning,
    pub coverage: PublicObservationCoverage,
}

impl JsonSchema for AgentObserveOutput {
    fn schema_name() -> Cow<'static, str> {
        "ZAS suspicion-only observation 4.3.1".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../schema/zas-observation-v1.1.schema.json"
        ))
        .expect("packaged observation schema must be valid JSON");
        value
            .as_object_mut()
            .expect("packaged observation schema must be an object")
            .remove("$schema");
        value
            .try_into()
            .expect("packaged observation schema must be a JSON Schema object")
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicObservedTool {
    #[schemars(length(min = 1))]
    pub tool_name: String,
    #[schemars(range(min = 1))]
    pub call_count: u64,
    #[schemars(length(min = 1, max = 5))]
    pub recent_calls: Vec<PublicObservedCall>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicObservedCall {
    #[schemars(range(min = 1))]
    pub seq: u64,
    #[schemars(length(min = 1))]
    pub tool_call_id: String,
    pub arguments: serde_json::Map<String, serde_json::Value>,
    pub arguments_truncated: bool,
    pub redacted_fields: u64,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicObservedReasoning {
    #[schemars(length(max = 200))]
    pub text: String,
    #[schemars(range(max = 200))]
    pub char_count: usize,
    pub truncated: bool,
    pub source: PublicReasoningSource,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicReasoningSource {
    pub status: PublicReasoningSourceStatus,
    #[schemars(length(min = 1))]
    pub runtime_version: String,
    #[schemars(length(min = 1))]
    pub event_type: String,
    #[schemars(regex(pattern = "^/"))]
    pub delta_pointer: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicObservationCoverage {
    pub tool_history_complete: bool,
    pub reasoning_complete: bool,
    pub dropped_events: u64,
}

impl TryFrom<TaskObservationView> for AgentObserveOutput {
    type Error = String;

    fn try_from(value: TaskObservationView) -> Result<Self, Self::Error> {
        if value.schema != "zas-observation/1.1"
            || value.count_scope != "agent_lifetime"
            || value.reasoning.source.status != "VERIFIED_RUNTIME_PUBLIC"
            || value.reasoning.source.runtime_version != "3.11.2"
            || value.reasoning.source.event_type != "model.streaming"
            || value.reasoning.source.delta_pointer != "/params/payload/delta"
        {
            return Err(protocol_error());
        }
        Ok(Self {
            schema: PublicObservationSchema::Version1_1,
            agent_id: value.agent_id,
            service_generation: value.service_generation,
            snapshot_seq: value.snapshot_seq,
            count_scope: PublicObservationCountScope::AgentLifetime,
            tools: value
                .tools
                .into_iter()
                .map(|tool| PublicObservedTool {
                    tool_name: tool.tool_name,
                    call_count: tool.call_count,
                    recent_calls: tool
                        .recent_calls
                        .into_iter()
                        .map(|call| PublicObservedCall {
                            seq: call.seq,
                            tool_call_id: call.tool_call_id,
                            arguments: call.arguments,
                            arguments_truncated: call.arguments_truncated,
                            redacted_fields: call.redacted_fields,
                        })
                        .collect(),
                })
                .collect(),
            reasoning: PublicObservedReasoning {
                text: value.reasoning.text,
                char_count: value.reasoning.char_count,
                truncated: value.reasoning.truncated,
                source: PublicReasoningSource {
                    status: PublicReasoningSourceStatus::VerifiedRuntimePublic,
                    runtime_version: value.reasoning.source.runtime_version,
                    event_type: value.reasoning.source.event_type,
                    delta_pointer: value.reasoning.source.delta_pointer,
                },
            },
            coverage: PublicObservationCoverage {
                tool_history_complete: value.coverage.tool_history_complete,
                reasoning_complete: value.coverage.reasoning_complete,
                dropped_events: value.coverage.dropped_events,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SystemStatusOutput {
    pub api_surface: String,
    pub protocol_version: u16,
    pub service_generation: String,
    pub components: BTreeMap<String, PublicComponentState>,
    pub capabilities: PublicAgentCapabilities,
}

impl From<SystemStatusView> for SystemStatusOutput {
    fn from(value: SystemStatusView) -> Self {
        Self {
            api_surface: value.api_surface,
            protocol_version: value.protocol_version,
            service_generation: value.service_generation,
            components: value
                .components
                .into_iter()
                .map(|(name, state)| (name, state.into()))
                .collect(),
            capabilities: value.capabilities.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentSpawnInput {
    pub repository: String,
    #[serde(default)]
    pub permission_mode: PublicPermissionMode,
    pub prompt: String,
    #[serde(default)]
    pub write_manifest: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionDisposition {
    Created,
    Existing,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentSpawnOutput {
    pub agent_id: String,
    pub submission_disposition: SubmissionDisposition,
    pub phase: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentInput {
    #[schemars(length(min = 1))]
    pub agent_id: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicTask {
    pub agent_id: String,
    pub phase: String,
    pub outcome: Option<PublicOutcome>,
    pub reason_code: Option<String>,
    pub cancel_requested: bool,
    pub close_requested: bool,
    pub closed: bool,
    pub resources_reaped: bool,
}

impl From<TaskView> for PublicTask {
    fn from(value: TaskView) -> Self {
        Self {
            agent_id: value.agent_id,
            phase: value.phase,
            outcome: value.outcome.map(Into::into),
            reason_code: value.reason_code,
            cancel_requested: value.stop_requested,
            close_requested: value.close_requested,
            closed: value.closed,
            resources_reaped: value.reaped,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicOutcome {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    RuntimeLost,
    ResultInvalid,
}

impl From<TaskOutcome> for PublicOutcome {
    fn from(value: TaskOutcome) -> Self {
        match value {
            TaskOutcome::Completed => Self::Completed,
            TaskOutcome::Failed => Self::Failed,
            TaskOutcome::Cancelled => Self::Cancelled,
            TaskOutcome::TimedOut => Self::TimedOut,
            TaskOutcome::RuntimeLost => Self::RuntimeLost,
            TaskOutcome::ResultInvalid => Self::ResultInvalid,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicResult {
    pub outcome: PublicOutcome,
    pub final_text: String,
    pub partial: bool,
    pub offset: usize,
    pub total_bytes: usize,
    pub next_offset: Option<usize>,
    pub complete: bool,
}

impl TryFrom<TaskResultView> for PublicResult {
    type Error = String;

    fn try_from(value: TaskResultView) -> Result<Self, Self::Error> {
        Ok(Self {
            outcome: value.outcome.into(),
            final_text: value.final_text,
            partial: value.partial,
            offset: value.offset,
            total_bytes: value.total_bytes,
            next_offset: value.next_offset,
            complete: value.complete,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentListInput {
    #[serde(default, deserialize_with = "optional_non_null")]
    pub repository: Option<String>,
    #[serde(default, deserialize_with = "optional_non_null")]
    pub phase: Option<PublicTaskPhase>,
    #[serde(default, deserialize_with = "optional_non_null")]
    pub outcome: Option<PublicOutcomeFilter>,
    #[serde(default, deserialize_with = "optional_non_null")]
    pub cursor: Option<String>,
    #[schemars(range(min = 1, max = 100))]
    #[serde(default = "default_list_limit")]
    pub limit: usize,
}

fn default_list_limit() -> usize {
    100
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicTaskPhase {
    Queued,
    Preparing,
    Running,
    WaitingInput,
    Cancelling,
    Terminal,
}

impl From<PublicTaskPhase> for TaskPhaseFilter {
    fn from(value: PublicTaskPhase) -> Self {
        match value {
            PublicTaskPhase::Queued => Self::Queued,
            PublicTaskPhase::Preparing => Self::Preparing,
            PublicTaskPhase::Running => Self::Running,
            PublicTaskPhase::WaitingInput => Self::WaitingInput,
            PublicTaskPhase::Cancelling => Self::Cancelling,
            PublicTaskPhase::Terminal => Self::Terminal,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicOutcomeFilter {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    RuntimeLost,
    ResultInvalid,
}

impl From<PublicOutcomeFilter> for TaskOutcome {
    fn from(value: PublicOutcomeFilter) -> Self {
        match value {
            PublicOutcomeFilter::Completed => Self::Completed,
            PublicOutcomeFilter::Failed => Self::Failed,
            PublicOutcomeFilter::Cancelled => Self::Cancelled,
            PublicOutcomeFilter::TimedOut => Self::TimedOut,
            PublicOutcomeFilter::RuntimeLost => Self::RuntimeLost,
            PublicOutcomeFilter::ResultInvalid => Self::ResultInvalid,
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentListOutput {
    pub tasks: Vec<PublicTask>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentPollInput {
    pub agent_id: String,
    #[serde(default)]
    pub after_revision: u64,
    #[serde(default)]
    #[schemars(range(min = 0, max = 5000))]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicActivityState {
    Queued,
    Preparing,
    Active,
    WaitingInput,
    Cancelling,
    Idle,
    Terminal,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicTelemetryStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicActivityToolKind {
    Read,
    Bash,
    Other,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicActiveTool {
    pub tool_call_id: String,
    pub kind: PublicActivityToolKind,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicActivityWindow {
    pub reasoning_delta_events: u64,
    pub reasoning_delta_bytes: u64,
    pub text_delta_events: u64,
    pub text_delta_bytes: u64,
    pub tool_calls_started: u64,
    pub tool_calls_completed: u64,
    pub tool_calls_failed: u64,
    pub read_calls: u64,
    pub bash_calls: u64,
    pub other_tool_calls: u64,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicActivity {
    pub state: PublicActivityState,
    pub last_runtime_event_at: Option<u64>,
    pub last_activity_age_ms: Option<u64>,
    pub model_request_active: bool,
    pub model_request_age_ms: Option<u64>,
    pub model_last_delta_age_ms: Option<u64>,
    pub latest_text_tail: String,
    pub latest_text_updated_at: Option<u64>,
    pub latest_text_truncated: bool,
    pub active_tools: Vec<PublicActiveTool>,
    pub window_60s: PublicActivityWindow,
    pub telemetry_status: PublicTelemetryStatus,
}

impl From<TaskActivityView> for PublicActivity {
    fn from(value: TaskActivityView) -> Self {
        Self {
            state: match value.state {
                TaskActivityStateView::Queued => PublicActivityState::Queued,
                TaskActivityStateView::Preparing => PublicActivityState::Preparing,
                TaskActivityStateView::Active => PublicActivityState::Active,
                TaskActivityStateView::WaitingInput => PublicActivityState::WaitingInput,
                TaskActivityStateView::Cancelling => PublicActivityState::Cancelling,
                TaskActivityStateView::Idle => PublicActivityState::Idle,
                TaskActivityStateView::Terminal => PublicActivityState::Terminal,
            },
            last_runtime_event_at: value.last_runtime_event_at,
            last_activity_age_ms: value.last_activity_age_ms,
            model_request_active: value.model_request_active,
            model_request_age_ms: value.model_request_age_ms,
            model_last_delta_age_ms: value.model_last_delta_age_ms,
            latest_text_tail: value.latest_text_tail,
            latest_text_updated_at: value.latest_text_updated_at,
            latest_text_truncated: value.latest_text_truncated,
            active_tools: value
                .active_tools
                .into_iter()
                .map(|tool| PublicActiveTool {
                    tool_call_id: tool.tool_call_id,
                    kind: match tool.kind {
                        zcode_agentd::rpc::ActivityToolKindView::Read => {
                            PublicActivityToolKind::Read
                        }
                        zcode_agentd::rpc::ActivityToolKindView::Bash => {
                            PublicActivityToolKind::Bash
                        }
                        zcode_agentd::rpc::ActivityToolKindView::Other => {
                            PublicActivityToolKind::Other
                        }
                    },
                })
                .collect(),
            window_60s: PublicActivityWindow {
                reasoning_delta_events: value.window_60s.reasoning_delta_events,
                reasoning_delta_bytes: value.window_60s.reasoning_delta_bytes,
                text_delta_events: value.window_60s.text_delta_events,
                text_delta_bytes: value.window_60s.text_delta_bytes,
                tool_calls_started: value.window_60s.tool_calls_started,
                tool_calls_completed: value.window_60s.tool_calls_completed,
                tool_calls_failed: value.window_60s.tool_calls_failed,
                read_calls: value.window_60s.read_calls,
                bash_calls: value.window_60s.bash_calls,
                other_tool_calls: value.window_60s.other_tool_calls,
            },
            telemetry_status: match value.telemetry_status {
                TelemetryStatusView::Healthy => PublicTelemetryStatus::Healthy,
                TelemetryStatusView::Degraded => PublicTelemetryStatus::Degraded,
                TelemetryStatusView::Unavailable => PublicTelemetryStatus::Unavailable,
            },
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentPollOutput {
    pub task: PublicTask,
    pub revision: u64,
    pub next_revision: u64,
    pub pending_requests: Vec<PublicPendingRequest>,
    pub command_pending_approval: bool,
    pub result_available: bool,
    pub activity: PublicActivity,
    pub latest_progress: Option<String>,
    pub result: Option<PublicResult>,
    pub instruction: Option<String>,
    pub timed_out: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentSendInput {
    pub agent_id: String,
    #[serde(default, deserialize_with = "optional_non_null")]
    pub message_id: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicMessageDisposition {
    Queued,
    Delivered,
    AlreadyDelivered,
    Failed,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentSendOutput {
    pub disposition: PublicMessageDisposition,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentRespondInput {
    pub agent_id: String,
    pub request_id: String,
    pub decision: PublicDecision,
    #[serde(default, deserialize_with = "optional_non_null")]
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentRespondOutput {
    pub disposition: PublicResponseDisposition,
    pub requested_decision: PublicDecision,
    pub effective_decision: PublicDecision,
    pub policy_overrode: bool,
    pub policy_reason_code: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentStateOutput {
    pub task: PublicTask,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentResultInput {
    pub agent_id: String,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_result_limit")]
    #[schemars(range(min = 1, max = 81920))]
    pub limit: usize,
}

fn default_result_limit() -> usize {
    zcode_agentd::rpc::MAX_RESULT_CHUNK_BYTES
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AgentResultOutput {
    pub task: PublicTask,
    pub result: Option<PublicResult>,
}

#[derive(Debug, Clone)]
pub struct SubagentMcp {
    socket: PathBuf,
    timeout: Duration,
    next_request: Arc<AtomicU64>,
    tool_router: ToolRouter<Self>,
}

impl SubagentMcp {
    pub fn new(socket: PathBuf, timeout: Duration) -> Self {
        Self {
            socket,
            timeout,
            next_request: Arc::new(AtomicU64::new(1)),
            tool_router: Self::tool_router(),
        }
    }

    fn generated_message_id(&self) -> String {
        format!("subagent-message-{}", Uuid::new_v4())
    }

    fn rpc(&self, method: RpcMethod) -> Result<RpcSuccess, String> {
        let request = RpcRequest {
            version: RPC_VERSION,
            request_id: format!(
                "subagent-mcp-{}",
                self.next_request.fetch_add(1, Ordering::Relaxed)
            ),
            method,
        };
        let encoded = serde_json::to_vec(&request)
            .map_err(|error| format!("validation: request encoding failed: {error}"))?;
        if encoded.len() + 1 > 512 * 1024 {
            return Err("validation: encoded RPC request exceeds frame cap".into());
        }
        let response = RpcClient::new(&self.socket, self.timeout)
            .call(&request)
            .map_err(public_transport_error)?;
        if response.version != RPC_VERSION {
            return Err("protocol_version_mismatch: incompatible agent daemon".into());
        }
        match response.outcome {
            RpcOutcome::Success { result } => Ok(*result),
            RpcOutcome::Error { error } => Err(public_error(error)),
        }
    }

    fn result(
        &self,
        agent_id: String,
        offset: usize,
        limit: usize,
    ) -> Result<(PublicTask, Option<PublicResult>), String> {
        match self.rpc(RpcMethod::TaskResult {
            agent_id,
            offset,
            limit,
        })? {
            RpcSuccess::TaskResult { task, result, .. } => {
                Ok((task.into(), result.map(TryInto::try_into).transpose()?))
            }
            _ => Err(protocol_error()),
        }
    }
}

fn general_manifest(input: &AgentSpawnInput) -> Result<GeneralTaskManifest, String> {
    for (field, value, max) in [
        ("repository", input.repository.as_str(), MAX_PATH_BYTES),
        ("prompt", input.prompt.as_str(), MAX_PROMPT_BYTES),
    ] {
        validate_text(value, field, max)?;
    }
    let repository = PathBuf::from(&input.repository);
    if !repository.is_absolute() {
        return Err("validation: repository must be absolute".into());
    }
    let agent_id = "daemon-prepared".to_owned();
    let mut write_manifest = Vec::with_capacity(input.write_manifest.len());
    for value in &input.write_manifest {
        validate_path(value, "write_manifest")?;
        let path = PathBuf::from(value);
        if path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::Prefix(_)
                        | std::path::Component::RootDir
                )
            })
        {
            return Err("validation: write_manifest paths must be relative to repository".into());
        }
        write_manifest.push(path);
    }
    if matches!(input.permission_mode, PublicPermissionMode::Plan) && !write_manifest.is_empty() {
        return Err("validation: write_manifest is only valid for write permission modes".into());
    }
    Ok(GeneralTaskManifest {
        schema: GENERAL_TASK_SCHEMA.into(),
        agent_id: agent_id.clone(),
        repository,
        permission_mode: input.permission_mode.into(),
        prompt: input.prompt.clone(),
        // Validate caller scope before the daemon applies its execution policy.
        write_manifest,
    })
}

fn project_response(value: ResponseOutcomeView) -> AgentRespondOutput {
    let requested_decision = if value.requested_decision == "allow" {
        PublicDecision::Allow
    } else {
        PublicDecision::Deny
    };
    let effective_decision = if value.effective_decision == "allow" {
        PublicDecision::Allow
    } else {
        PublicDecision::Deny
    };
    let disposition = match value.disposition {
        zcode_agentd::rpc::ResponseDispositionView::Responded => {
            PublicResponseDisposition::Responded
        }
        zcode_agentd::rpc::ResponseDispositionView::AlreadyResponded => {
            PublicResponseDisposition::AlreadyResponded
        }
        zcode_agentd::rpc::ResponseDispositionView::InFlight => PublicResponseDisposition::InFlight,
    };
    AgentRespondOutput {
        disposition,
        requested_decision,
        effective_decision,
        policy_overrode: value.policy_overrode,
        policy_reason_code: value.policy_reason_code,
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SubagentMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_instructions("Stateless public facade for durable local ZCode subagent tasks")
    }
}

#[tool_router(router = tool_router)]
impl SubagentMcp {
    #[tool(
        name = "zcode_subagent_status",
        description = "Read daemon/runtime readiness, protocol version, component states, and capability limits. Read-only.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn system_status(
        &self,
        Parameters(_): Parameters<EmptyInput>,
    ) -> Result<Json<SystemStatusOutput>, String> {
        match self.rpc(RpcMethod::SystemStatus)? {
            RpcSuccess::SystemStatus { status } => Ok(Json(status.into())),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_spawn",
        description = "Start one durable Agent in an absolute repository workspace. permission_mode defaults to build; an omitted write_manifest uses the protected workspace scope. Poll the returned agent_id for progress and terminal diagnostics.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn agent_spawn(
        &self,
        Parameters(input): Parameters<AgentSpawnInput>,
    ) -> Result<Json<AgentSpawnOutput>, String> {
        let manifest = general_manifest(&input)?;
        let (task, disposition) = match self.rpc(RpcMethod::SubmitGeneral {
            input: GeneralSubmitInput { manifest },
        })? {
            RpcSuccess::GeneralSubmitted { task, disposition } => (task, disposition),
            _ => return Err(protocol_error()),
        };
        Ok(Json(AgentSpawnOutput {
            agent_id: task.agent_id,
            submission_disposition: match disposition {
                SubmissionDispositionView::Created => SubmissionDisposition::Created,
                SubmissionDispositionView::Existing => SubmissionDisposition::Existing,
            },
            phase: task.phase,
        }))
    }

    #[tool(
        name = "zcode_subagent_poll",
        description = "Long-poll a task revision with typed pending requests and passive runtime activity",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_poll(
        &self,
        Parameters(input): Parameters<AgentPollInput>,
    ) -> Result<Json<AgentPollOutput>, String> {
        validate_text(&input.agent_id, "agent_id", MAX_ID_BYTES)?;
        if input.timeout_ms > 5000 {
            return Err("validation: timeout_ms must be between 0 and 5000".into());
        }
        match self.rpc(RpcMethod::TaskPoll(TaskPollQuery {
            agent_id: input.agent_id,
            after_revision: input.after_revision,
            timeout_ms: input.timeout_ms,
        }))? {
            RpcSuccess::TaskPoll {
                task,
                revision,
                next_revision,
                pending_requests,
                command_pending_approval,
                result_available,
                activity,
                latest_progress,
                result,
                instruction,
                timed_out,
            } => Ok(Json(AgentPollOutput {
                task: task.into(),
                revision,
                next_revision,
                pending_requests: pending_requests.into_iter().map(Into::into).collect(),
                command_pending_approval,
                result_available,
                activity: activity.into(),
                latest_progress,
                result: result.map(TryInto::try_into).transpose()?,
                instruction,
                timed_out,
            })),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_observe",
        description = "仅在怀疑 zcode subagent 陷入无意义循环时才调用，检查最近公开推理和工具调用过程；不要用于健康任务的例行轮询。只读已捕获的本 Agent 数据，不启动模型或工具。默认按本 Agent 任务生命周期内的调用次数选最多的 3 类工具，每类返回最近最多 5 次调用（名称、ID、参数，不含结果），并返回已验证公开 reasoning delta 合并后的最新 200 个 Unicode 字符。encrypted_content 始终排除。ZAS 不判断循环、不返回进展标签、不自动取消。调用方结合当前任务与这些事实判断：PROGRESSING（新增事实或有效推进）；EXPECTED_WAIT（有目的的计算、权限或外部等待）；NEEDS_CLARIFICATION（具体输入或决定缺失）；NO_PROGRESS_LOOP（无新信息的等价行动循环，且合理重读、等待、状态变化等解释已排除）；INSUFFICIENT_OBSERVABILITY（截断、缺口或缺少上下文，不能断言循环）。相同文本、重复 read 或 true/echo 本身不是循环；没有工具结果也不能推断工具成功、文件未变化或任务失败。判断和取消由调用方独立决定。",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_observe(
        &self,
        Parameters(input): Parameters<AgentInput>,
    ) -> Result<Json<AgentObserveOutput>, String> {
        validate_text(&input.agent_id, "agent_id", MAX_ID_BYTES)?;
        match self.rpc(RpcMethod::TaskObserve {
            agent_id: input.agent_id,
        })? {
            RpcSuccess::TaskObserved { observation } => Ok(Json(observation.try_into()?)),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_list",
        description = "List tasks within an explicit daemon-enforced scope",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_list(
        &self,
        Parameters(input): Parameters<AgentListInput>,
    ) -> Result<Json<AgentListOutput>, String> {
        if !(1..=100).contains(&input.limit) {
            return Err("validation: limit must be between 1 and 100".into());
        }
        if input.repository.is_none() {
            return Err("validation: at least one list scope is required".into());
        }
        match self.rpc(RpcMethod::TaskList(TaskListQuery {
            repository: input.repository,
            phase: input.phase.map(Into::into),
            outcome: input.outcome.map(Into::into),
            cursor: input.cursor,
            limit: input.limit,
        }))? {
            RpcSuccess::TaskListed { tasks, next_cursor } => Ok(Json(AgentListOutput {
                tasks: tasks.into_iter().map(Into::into).collect(),
                next_cursor,
            })),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_send",
        description = "Queue a bounded message for a running task",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn agent_send(
        &self,
        Parameters(input): Parameters<AgentSendInput>,
    ) -> Result<Json<AgentSendOutput>, String> {
        validate_text(&input.content, "content", MAX_MESSAGE_BYTES)?;
        let message_id = input
            .message_id
            .unwrap_or_else(|| self.generated_message_id());
        match self.rpc(RpcMethod::TaskMessage(MessageInput {
            agent_id: input.agent_id.clone(),
            message_id,
            mode: "queue".into(),
            content: input.content,
        }))? {
            RpcSuccess::Message { disposition, .. } => Ok(Json(AgentSendOutput {
                disposition: match disposition {
                    zcode_agentd::rpc::MessageDispositionView::Queued => {
                        PublicMessageDisposition::Queued
                    }
                    zcode_agentd::rpc::MessageDispositionView::Delivered => {
                        PublicMessageDisposition::Delivered
                    }
                    zcode_agentd::rpc::MessageDispositionView::AlreadyDelivered => {
                        PublicMessageDisposition::AlreadyDelivered
                    }
                    zcode_agentd::rpc::MessageDispositionView::Failed => {
                        PublicMessageDisposition::Failed
                    }
                },
            })),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_respond",
        description = "Respond idempotently to a typed pending permission request",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_respond(
        &self,
        Parameters(input): Parameters<AgentRespondInput>,
    ) -> Result<Json<AgentRespondOutput>, String> {
        if input.reason.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > MAX_REASON_BYTES || value.contains('\0')
        }) {
            return Err("validation: reason is invalid".into());
        }
        let decision = match input.decision {
            PublicDecision::Allow => ResponseDecision::Allow,
            PublicDecision::Deny => ResponseDecision::Deny,
        };
        match self.rpc(RpcMethod::TaskRespond(RespondInput {
            agent_id: input.agent_id.clone(),
            request_id: input.request_id,
            decision,
            content: input.reason,
        }))? {
            RpcSuccess::Respond { outcome, .. } => Ok(Json(project_response(outcome))),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_cancel",
        description = "Cancel a task without removing durable history",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_cancel(
        &self,
        Parameters(input): Parameters<AgentInput>,
    ) -> Result<Json<AgentStateOutput>, String> {
        match self.rpc(RpcMethod::TaskCancel {
            agent_id: input.agent_id.clone(),
        })? {
            RpcSuccess::Stopped { task } => Ok(Json(AgentStateOutput { task: task.into() })),
            _ => Err(protocol_error()),
        }
    }

    #[tool(
        name = "zcode_subagent_result",
        description = "Read a terminal task result with stable outcome, partial status, reason code, and bounded final-text segments. Returns null result while the task is non-terminal.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_result(
        &self,
        Parameters(input): Parameters<AgentResultInput>,
    ) -> Result<Json<AgentResultOutput>, String> {
        let (task, result) = self.result(input.agent_id.clone(), input.offset, input.limit)?;
        Ok(Json(AgentResultOutput { task, result }))
    }

    #[tool(
        name = "zcode_subagent_close",
        description = "Close a task and reap runtime resources while preserving durable history",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn agent_close(
        &self,
        Parameters(input): Parameters<AgentInput>,
    ) -> Result<Json<AgentStateOutput>, String> {
        match self.rpc(RpcMethod::TaskClose {
            agent_id: input.agent_id.clone(),
        })? {
            RpcSuccess::Closed { task } => Ok(Json(AgentStateOutput { task: task.into() })),
            _ => Err(protocol_error()),
        }
    }
}

pub async fn serve_stdio(
    socket: PathBuf,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    SubagentMcp::new(socket, timeout)
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await?
        .waiting()
        .await?;
    Ok(())
}

#[cfg(test)]
mod contract_default_tests {
    use super::{
        default_result_limit, AgentListInput, AgentObserveOutput, AgentPollInput, AgentResultInput,
        AgentSendInput, SubagentMcp, PUBLIC_TOOLS,
    };
    use sha2::{Digest, Sha256};
    use std::{collections::HashSet, path::PathBuf, time::Duration};
    use zcode_agentd::{observation::ObservationSnapshot, rpc::TaskObservationView};

    #[test]
    fn omitted_public_fields_use_the_frozen_defaults() {
        let list: AgentListInput = serde_json::from_value(serde_json::json!({
            "repository": "/tmp/repository"
        }))
        .unwrap();
        assert_eq!(list.limit, 100);
        assert!(list.phase.is_none());
        assert!(list.outcome.is_none());
        assert!(list.cursor.is_none());

        let poll: AgentPollInput =
            serde_json::from_value(serde_json::json!({"agent_id": "agent"})).unwrap();
        assert_eq!(poll.after_revision, 0);
        assert_eq!(poll.timeout_ms, 0);

        let result: AgentResultInput =
            serde_json::from_value(serde_json::json!({"agent_id": "agent"})).unwrap();
        assert_eq!(result.offset, 0);
        assert_eq!(result.limit, default_result_limit());

        let send: AgentSendInput = serde_json::from_value(serde_json::json!({
            "agent_id": "agent",
            "content": "continue"
        }))
        .unwrap();
        assert!(send.message_id.is_none());
    }

    #[test]
    fn generated_message_ids_are_unique_across_facades_and_restart() {
        let facade_a = SubagentMcp::new(PathBuf::from("/tmp/a.sock"), Duration::from_secs(1));
        let facade_b = SubagentMcp::new(PathBuf::from("/tmp/b.sock"), Duration::from_secs(1));
        let id_a = facade_a.generated_message_id();
        let id_b = facade_b.generated_message_id();
        let restarted = SubagentMcp::new(PathBuf::from("/tmp/a.sock"), Duration::from_secs(1));
        let ids = [
            id_a,
            id_b,
            restarted.generated_message_id(),
            restarted.generated_message_id(),
        ];
        assert_eq!(ids.len(), ids.iter().collect::<HashSet<_>>().len());
        assert_eq!(
            restarted
                .next_request
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn observation_tool_catalog_matches_the_frozen_description_and_bounds() {
        let facade = SubagentMcp::new(PathBuf::from("/tmp/observe.sock"), Duration::from_secs(1));
        let tools = facade.tool_router.list_all();
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.name.as_ref())
                .collect::<Vec<_>>(),
            PUBLIC_TOOLS
        );
        let observe = tools
            .iter()
            .find(|tool| tool.name == "zcode_subagent_observe")
            .unwrap();
        let description = observe.description.as_deref().unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(description.as_bytes())),
            "27da41da0527693fa8841d1604c4803c5fa31602a596ab1f73edc7c5a3535f84"
        );
        let input = serde_json::to_value(&observe.input_schema).unwrap();
        assert_eq!(input["additionalProperties"], false);
        assert_eq!(input["required"], serde_json::json!(["agent_id"]));
        let output = serde_json::to_value(observe.output_schema.as_ref().unwrap()).unwrap();
        assert_eq!(output["additionalProperties"], false);
        assert_eq!(
            output["properties"]["schema"]["const"],
            "zas-observation/1.1"
        );
        assert_eq!(
            output["properties"]["count_scope"]["const"],
            "agent_lifetime"
        );
        assert_eq!(output["properties"]["tools"]["maxItems"], 3);
        assert_eq!(
            output["properties"]["tools"]["items"]["properties"]["recent_calls"]["maxItems"],
            5
        );
        assert_eq!(
            output["properties"]["reasoning"]["properties"]["text"]["maxLength"],
            200
        );
    }

    #[test]
    fn observation_projection_rejects_source_contract_drift() {
        let snapshot = ObservationSnapshot::unavailable();
        let view = TaskObservationView {
            schema: "zas-observation/1.1".into(),
            agent_id: "agent".into(),
            service_generation: "generation".into(),
            snapshot_seq: snapshot.snapshot_seq,
            count_scope: "agent_lifetime".into(),
            tools: snapshot.tools,
            reasoning: snapshot.reasoning,
            coverage: snapshot.coverage,
        };
        assert!(AgentObserveOutput::try_from(view.clone()).is_ok());
        let mut drifted = view;
        drifted.reasoning.source.delta_pointer = "/private/field".into();
        assert!(AgentObserveOutput::try_from(drifted).is_err());
    }
}
