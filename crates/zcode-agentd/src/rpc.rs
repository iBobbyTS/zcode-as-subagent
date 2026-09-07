use crate::{
    MessageDisposition, PassiveActivitySnapshot, PassiveActivityWindow, PassiveToolKind,
    ResponseDisposition, Scheduler, SchedulerError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use zcode_agent_preparation::{
    canonical_general_repository, GeneralTaskManifest, PreparedGeneralTask,
};
use zcode_agent_store::{
    PendingRequestState, Store, StoreError, StoredPendingRequest, StoredTaskResult, TaskOutcome,
    TaskPageFilter, TaskPhase, TaskQueryScope, TaskRecord, TaskSubmissionDisposition,
};

pub const RPC_VERSION: u16 = 12;
pub const MAX_FRAME_BYTES: usize = 512 * 1024;
const MAX_REQUEST_ID_BYTES: usize = 128;
pub const MAX_LIST_TASKS: usize = 100;
pub const MAX_PENDING_REQUESTS: usize = 100;
/// A result page is capped below the transport frame cap so that even the
/// worst-case JSON escaping (one input byte becoming a six-byte `\\u00XX`
/// escape), the response envelope, and the trailing newline fit in one frame.
pub const MAX_RESULT_CHUNK_BYTES: usize = 80 * 1024;
pub const MAX_WAIT: Duration = Duration::from_secs(5);
pub const RPC_TRANSPORT_SUPPORTED: bool = cfg!(unix);

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::{RpcClient, RpcServer, ServerOptions};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RpcRequest {
    pub version: u16,
    pub request_id: String,
    #[serde(flatten)]
    pub method: RpcMethod,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "method",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[allow(clippy::large_enum_variant)]
pub enum RpcMethod {
    SystemStatus,
    SubmitGeneral {
        input: GeneralSubmitInput,
    },
    TaskList(TaskListQuery),
    TaskPoll(TaskPollQuery),
    TaskMessage(MessageInput),
    TaskRespond(RespondInput),
    TaskCancel {
        agent_id: String,
    },
    TaskResult {
        agent_id: String,
        #[serde(default)]
        offset: usize,
        #[serde(default = "default_result_limit")]
        limit: usize,
    },
    TaskClose {
        agent_id: String,
    },
}

impl RpcMethod {
    fn is_known(name: &str) -> bool {
        matches!(
            name,
            "system_status"
                | "submit_general"
                | "task_list"
                | "task_poll"
                | "task_message"
                | "task_respond"
                | "task_cancel"
                | "task_result"
                | "task_close"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskListQuery {
    #[serde(default)]
    pub repository: Option<String>,
    #[serde(default)]
    pub phase: Option<TaskPhaseFilter>,
    #[serde(default)]
    pub outcome: Option<TaskOutcome>,
    #[serde(default)]
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskPhaseFilter {
    Queued,
    Preparing,
    Running,
    WaitingInput,
    Cancelling,
    Terminal,
}

impl From<TaskPhaseFilter> for TaskPhase {
    fn from(value: TaskPhaseFilter) -> Self {
        match value {
            TaskPhaseFilter::Queued => Self::Queued,
            TaskPhaseFilter::Preparing => Self::Preparing,
            TaskPhaseFilter::Running => Self::Running,
            TaskPhaseFilter::WaitingInput => Self::WaitingInput,
            TaskPhaseFilter::Cancelling => Self::Cancelling,
            TaskPhaseFilter::Terminal => Self::Terminal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneralSubmitInput {
    pub manifest: GeneralTaskManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPollQuery {
    pub agent_id: String,
    #[serde(default)]
    pub after_revision: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageInput {
    pub agent_id: String,
    pub message_id: String,
    pub mode: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RespondInput {
    pub agent_id: String,
    pub request_id: String,
    pub decision: ResponseDecision,
    #[serde(default)]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseDecision {
    Allow,
    Deny,
    Answer,
}

impl ResponseDecision {
    fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Answer => "answer",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcResponse {
    pub version: u16,
    pub request_id: Option<String>,
    #[serde(flatten)]
    pub outcome: RpcOutcome,
}

impl RpcResponse {
    pub fn success(request_id: String, result: RpcSuccess) -> Self {
        Self {
            version: RPC_VERSION,
            request_id: Some(request_id),
            outcome: RpcOutcome::Success {
                result: Box::new(result),
            },
        }
    }

    pub fn error(request_id: Option<String>, error: RpcError) -> Self {
        Self {
            version: RPC_VERSION,
            request_id,
            outcome: RpcOutcome::Error { error },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RpcOutcome {
    Success { result: Box<RpcSuccess> },
    Error { error: RpcError },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RpcSuccess {
    SystemStatus {
        status: SystemStatusView,
    },
    GeneralSubmitted {
        task: TaskView,
        disposition: SubmissionDispositionView,
    },
    TaskListed {
        tasks: Vec<TaskView>,
        next_cursor: Option<String>,
    },
    TaskPoll {
        task: TaskView,
        revision: u64,
        next_revision: u64,
        pending_requests: Vec<PendingRequestView>,
        command_pending_approval: bool,
        result_available: bool,
        activity: TaskActivityView,
        latest_progress: Option<String>,
        result: Option<TaskResultView>,
        instruction: Option<String>,
        timed_out: bool,
    },
    TaskResult {
        task: TaskView,
        result: Option<TaskResultView>,
    },
    Message {
        disposition: MessageDispositionView,
        task: TaskView,
    },
    Respond {
        outcome: ResponseOutcomeView,
        task: TaskView,
    },
    Stopped {
        task: TaskView,
    },
    Closed {
        task: TaskView,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentStateView {
    Ready,
    Degraded,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityMaturityView {
    BetaReady,
    ExperimentalUnverifiedRuntime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionDispositionView {
    Created,
    Existing,
}

impl From<TaskSubmissionDisposition> for SubmissionDispositionView {
    fn from(value: TaskSubmissionDisposition) -> Self {
        match value {
            TaskSubmissionDisposition::Created => Self::Created,
            TaskSubmissionDisposition::Existing => Self::Existing,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemStatusView {
    pub api_surface: String,
    pub protocol_version: u16,
    pub service_generation: String,
    pub components: BTreeMap<String, ComponentStateView>,
    pub capabilities: AgentCapabilitiesView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCapabilitiesView {
    pub max_rpc_frame_bytes: usize,
    pub max_wait_ms: u64,
    pub maturity: BTreeMap<String, CapabilityMaturityView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskView {
    pub agent_id: String,
    pub phase: String,
    pub outcome: Option<TaskOutcome>,
    pub reason_code: Option<String>,
    pub stop_requested: bool,
    pub close_requested: bool,
    pub closed: bool,
    pub reaped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskActivityStateView {
    Queued,
    Preparing,
    Active,
    WaitingInput,
    Cancelling,
    Idle,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryStatusView {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityToolKindView {
    Read,
    Bash,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveToolView {
    pub tool_call_id: String,
    pub kind: ActivityToolKindView,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityWindowView {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskActivityView {
    pub state: TaskActivityStateView,
    pub last_runtime_event_at: Option<u64>,
    pub last_activity_age_ms: Option<u64>,
    pub model_request_active: bool,
    pub model_request_age_ms: Option<u64>,
    pub model_last_delta_age_ms: Option<u64>,
    pub latest_text_tail: String,
    pub diagnostic_tail: String,
    pub latest_text_updated_at: Option<u64>,
    pub latest_text_truncated: bool,
    pub latest_progress: Option<String>,
    pub active_tools: Vec<ActiveToolView>,
    pub window_60s: ActivityWindowView,
    pub telemetry_status: TelemetryStatusView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResultView {
    pub outcome: TaskOutcome,
    pub final_text: String,
    pub partial: bool,
    pub result_sha256: String,
    pub offset: usize,
    pub total_bytes: usize,
    pub next_offset: Option<usize>,
    pub complete: bool,
}

fn default_result_limit() -> usize {
    MAX_RESULT_CHUNK_BYTES
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcErrorCode {
    Malformed,
    Oversized,
    UnsupportedVersion,
    UnknownMethod,
    Validation,
    NotFound,
    Conflict,
    Persistence,
    Timeout,
    RuntimeLost,
    ResultInvalid,
    Unavailable,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: RpcErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_agent_id: Option<String>,
}

impl RpcError {
    pub fn new(code: RpcErrorCode, message: impl Into<String>) -> Self {
        let mut message = message.into();
        message.truncate(512);
        Self {
            code,
            message,
            active_agent_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageDispositionView {
    Queued,
    Delivered,
    AlreadyDelivered,
    Failed,
}

impl From<MessageDisposition> for MessageDispositionView {
    fn from(value: MessageDisposition) -> Self {
        match value {
            MessageDisposition::Queued => Self::Queued,
            MessageDisposition::Delivered => Self::Delivered,
            MessageDisposition::AlreadyDelivered => Self::AlreadyDelivered,
            MessageDisposition::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseDispositionView {
    Responded,
    AlreadyResponded,
    InFlight,
}

impl From<ResponseDisposition> for ResponseDispositionView {
    fn from(value: ResponseDisposition) -> Self {
        match value {
            ResponseDisposition::Responded => Self::Responded,
            ResponseDisposition::AlreadyResponded => Self::AlreadyResponded,
            ResponseDisposition::InFlight => Self::InFlight,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseOutcomeView {
    pub disposition: ResponseDispositionView,
    pub requested_decision: String,
    pub effective_decision: String,
    pub policy_overrode: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_reason_code: Option<String>,
}

impl From<crate::ResponseOutcome> for ResponseOutcomeView {
    fn from(value: crate::ResponseOutcome) -> Self {
        Self {
            disposition: value.disposition.into(),
            requested_decision: value.requested_decision,
            effective_decision: value.effective_decision,
            policy_overrode: value.policy_overrode,
            policy_reason_code: value.policy_reason_code,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRequestStateView {
    Pending,
    Sending,
    Responded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRequestView {
    pub request_id: String,
    pub kind: String,
    pub state: PendingRequestStateView,
    pub respondable: bool,
    pub tool_name: Option<String>,
    pub operation: String,
    pub summary: String,
    pub policy_preview: String,
}

#[derive(Clone)]
pub struct RpcService {
    scheduler: Scheduler,
    store: Arc<Store>,
    service_generation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcServiceConfigError {
    MismatchedStore,
    GenerationUnavailable,
}

impl RpcService {
    pub fn new(scheduler: Scheduler, store: Arc<Store>) -> Result<Self, RpcServiceConfigError> {
        let service_generation = opaque_generation()?;
        Self::new_with_service_generation(scheduler, store, service_generation)
    }

    pub(crate) fn new_with_service_generation(
        scheduler: Scheduler,
        store: Arc<Store>,
        service_generation: String,
    ) -> Result<Self, RpcServiceConfigError> {
        if !Arc::ptr_eq(&scheduler.store(), &store) {
            return Err(RpcServiceConfigError::MismatchedStore);
        }
        Ok(Self {
            scheduler,
            store,
            service_generation,
        })
    }

    pub fn handle_bytes(&self, frame: &[u8]) -> RpcResponse {
        if frame.len() > MAX_FRAME_BYTES {
            return RpcResponse::error(
                None,
                RpcError::new(RpcErrorCode::Oversized, "request frame exceeds the RPC cap"),
            );
        }
        let value = match serde_json::from_slice::<Value>(frame) {
            Ok(value) => value,
            Err(_) => {
                return RpcResponse::error(
                    None,
                    RpcError::new(RpcErrorCode::Malformed, "request is not valid JSON"),
                )
            }
        };
        let request_id = value
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|request_id| valid_request_id(request_id))
            .map(str::to_owned);
        let version = value.get("version").and_then(Value::as_u64);
        if version != Some(u64::from(RPC_VERSION)) {
            return RpcResponse::error(
                request_id,
                RpcError::new(
                    RpcErrorCode::UnsupportedVersion,
                    "unsupported RPC protocol version",
                ),
            );
        }
        if value.as_object().is_none_or(|object| {
            object
                .keys()
                .any(|key| !matches!(key.as_str(), "version" | "request_id" | "method" | "params"))
        }) {
            return RpcResponse::error(
                request_id,
                RpcError::new(RpcErrorCode::Validation, "request fields are invalid"),
            );
        }
        let method = value.get("method").and_then(Value::as_str);
        if let Some(method) = method {
            if !RpcMethod::is_known(method) {
                return RpcResponse::error(
                    request_id,
                    RpcError::new(RpcErrorCode::UnknownMethod, "unknown RPC method"),
                );
            }
        }
        let request = match serde_json::from_value::<RpcRequest>(value) {
            Ok(request) => request,
            Err(_) => {
                return RpcResponse::error(
                    request_id,
                    RpcError::new(RpcErrorCode::Validation, "request fields are invalid"),
                )
            }
        };
        if !valid_request_id(&request.request_id) {
            return RpcResponse::error(
                None,
                RpcError::new(RpcErrorCode::Validation, "request_id is invalid"),
            );
        }
        let request_id = request.request_id;
        match self.dispatch(request.method) {
            Ok(result) => RpcResponse::success(request_id, result),
            Err(error) => RpcResponse::error(Some(request_id), error),
        }
    }

    pub fn dispatch(&self, method: RpcMethod) -> Result<RpcSuccess, RpcError> {
        match method {
            RpcMethod::SystemStatus => Ok(RpcSuccess::SystemStatus {
                status: self.system_status(),
            }),
            RpcMethod::SubmitGeneral { input } => {
                let manifest = input.manifest;
                let submitted = self
                    .scheduler
                    .enqueue_general(&manifest)
                    .map_err(map_scheduler)?;
                Ok(RpcSuccess::GeneralSubmitted {
                    task: task_view(submitted.task),
                    disposition: submitted.disposition.into(),
                })
            }
            RpcMethod::TaskList(query) => {
                if query.limit == 0 || query.limit > MAX_LIST_TASKS {
                    return Err(RpcError::new(
                        RpcErrorCode::Validation,
                        "task list limit is outside the allowed range",
                    ));
                }
                for (field, value, cap) in [("repository", query.repository.as_deref(), 4096usize)]
                {
                    if let Some(value) = value {
                        validate_text(value, field, cap)?;
                    }
                }
                if let Some(cursor) = query.cursor.as_deref() {
                    validate_text(cursor, "cursor", 64)?;
                }
                if query.repository.is_none() {
                    return Err(RpcError::new(
                        RpcErrorCode::Validation,
                        "at least one task list scope is required",
                    ));
                }
                let canonical_repository = query
                    .repository
                    .as_deref()
                    .map(|repository| canonical_general_repository(Path::new(repository)))
                    .transpose()
                    .map_err(|_| {
                        RpcError::new(RpcErrorCode::Validation, "repository scope is invalid")
                    })?
                    .map(|repository| repository.to_string_lossy().into_owned());
                let page = self
                    .store
                    .list_task_page(
                        TaskQueryScope {
                            repository: canonical_repository.as_deref(),
                        },
                        TaskPageFilter {
                            phase: query.phase.map(Into::into),
                            outcome: query.outcome,
                        },
                        query.cursor.as_deref().map(parse_task_cursor).transpose()?,
                        query.limit,
                    )
                    .map_err(map_store)?;
                let mut views = Vec::with_capacity(page.tasks.len());
                for task in page.tasks {
                    views.push(task_view(task));
                }
                Ok(RpcSuccess::TaskListed {
                    tasks: views,
                    next_cursor: page.next_cursor.map(format_task_cursor),
                })
            }
            RpcMethod::TaskPoll(query) => self.task_poll(query),
            RpcMethod::TaskMessage(input) => {
                let task = self.require_task(&input.agent_id)?;
                validate_id(&input.message_id, "message_id")?;
                // The generic control plane only queues clarification. A
                // terminal task may be resumed by the scheduler when the
                // persisted ZCode session accepts a restore; other
                // interrupt-and-continue paths remain private.
                if input.mode != "queue" {
                    return Err(RpcError::new(
                        RpcErrorCode::Validation,
                        "generic agent messages must use queue mode",
                    ));
                }
                validate_text(&input.content, "content", 16 * 1024)?;
                let disposition = self
                    .scheduler
                    .queue_message(
                        &task.agent_id,
                        &input.message_id,
                        &input.mode,
                        &input.content,
                    )
                    .map_err(map_scheduler)?;
                let task = self.require_task(&input.agent_id)?;
                Ok(RpcSuccess::Message {
                    disposition: disposition.into(),
                    task: task_view(task),
                })
            }
            RpcMethod::TaskRespond(input) => {
                let task = self.require_task(&input.agent_id)?;
                validate_id(&input.request_id, "request_id")?;
                if let Some(content) = input.content.as_deref() {
                    validate_text(content, "response content", 16 * 1024)?;
                }
                let outcome = self
                    .scheduler
                    .respond_request(
                        &task.agent_id,
                        &input.request_id,
                        input.decision.as_str(),
                        input.content.as_deref(),
                    )
                    .map_err(map_scheduler)?;
                let task = self.require_task(&input.agent_id)?;
                Ok(RpcSuccess::Respond {
                    outcome: outcome.into(),
                    task: task_view(task),
                })
            }
            RpcMethod::TaskCancel { agent_id } => {
                let task = self.require_task(&agent_id)?;
                self.scheduler
                    .cancel_task(&task.agent_id)
                    .map_err(map_scheduler)?;
                let task = self.require_task(&agent_id)?;
                Ok(RpcSuccess::Stopped {
                    task: task_view(task),
                })
            }
            RpcMethod::TaskResult {
                agent_id,
                offset,
                limit,
            } => {
                let task = self.require_task(&agent_id)?;
                if limit == 0 || limit > MAX_RESULT_CHUNK_BYTES {
                    return Err(RpcError::new(
                        RpcErrorCode::Validation,
                        "result limit is outside the allowed range",
                    ));
                }
                let result = self
                    .store
                    .task_result(&task.agent_id)
                    .map_err(map_store)?
                    .map(|stored| self.task_result_view(stored, offset, limit))
                    .transpose()?;
                Ok(RpcSuccess::TaskResult {
                    task: task_view(task),
                    result,
                })
            }
            RpcMethod::TaskClose { agent_id } => {
                let task = self.require_task(&agent_id)?;
                self.scheduler
                    .close_task(&task.agent_id)
                    .map_err(map_scheduler)?;
                let task = self.require_task(&agent_id)?;
                Ok(RpcSuccess::Closed {
                    task: task_view(task),
                })
            }
        }
    }

    fn system_status(&self) -> SystemStatusView {
        let mut components = BTreeMap::new();
        components.insert("facade".into(), ComponentStateView::Unknown);
        components.insert("daemon".into(), ComponentStateView::Ready);
        components.insert(
            "store".into(),
            match self.store.journal_mode() {
                Ok(mode) if mode.eq_ignore_ascii_case("wal") => ComponentStateView::Ready,
                Ok(_) => ComponentStateView::Degraded,
                Err(_) => ComponentStateView::Unavailable,
            },
        );
        components.insert("scheduler".into(), ComponentStateView::Ready);
        components.insert("driver".into(), ComponentStateView::Unknown);
        components.insert("runtime".into(), ComponentStateView::Unknown);
        components.insert("model_auth".into(), ComponentStateView::Unknown);
        SystemStatusView {
            api_surface: "generic_agent".into(),
            protocol_version: RPC_VERSION,
            service_generation: self.service_generation.clone(),
            components,
            capabilities: agent_capabilities(),
        }
    }

    fn require_task(&self, agent_id: &str) -> Result<TaskRecord, RpcError> {
        validate_id(agent_id, "agent_id")?;
        let task = self
            .store
            .get_task(agent_id)
            .map_err(map_store)?
            .ok_or_else(|| RpcError::new(RpcErrorCode::NotFound, "task was not found"))?;
        let prepared = serde_json::from_str::<PreparedGeneralTask>(&task.prepared_launch_json)
            .map_err(|_| RpcError::new(RpcErrorCode::NotFound, "task was not found"))?;
        if prepared.repository.to_string_lossy() != task.repository {
            return Err(RpcError::new(RpcErrorCode::NotFound, "task was not found"));
        }
        Ok(task)
    }

    fn task_result_view(
        &self,
        stored: StoredTaskResult,
        offset: usize,
        limit: usize,
    ) -> Result<TaskResultView, RpcError> {
        let text = stored.result.final_text;
        let total_bytes = text.len();
        let (end, next_offset) = result_page_bounds(&text, offset, limit)?;
        Ok(TaskResultView {
            outcome: stored.result.outcome,
            final_text: text[offset..end].to_owned(),
            partial: stored.result.partial,
            result_sha256: stored.result_sha256,
            offset,
            total_bytes,
            next_offset,
            complete: next_offset.is_none(),
        })
    }

    fn task_poll(&self, query: TaskPollQuery) -> Result<RpcSuccess, RpcError> {
        if Duration::from_millis(query.timeout_ms) > MAX_WAIT {
            return Err(RpcError::new(
                RpcErrorCode::Validation,
                "poll timeout is outside the allowed range",
            ));
        }
        let deadline = Instant::now() + Duration::from_millis(query.timeout_ms);
        loop {
            let task = self.require_task(&query.agent_id)?;
            let pending_requests = self
                .store
                .pending_requests_bounded(&task.agent_id, MAX_PENDING_REQUESTS)
                .map_err(map_store)?
                .into_iter()
                .map(pending_request_view)
                .collect::<Vec<_>>();
            let command_pending_approval = pending_requests.iter().any(|request| {
                request.kind == "permission" && request.state == PendingRequestStateView::Pending
            });
            let result_available = self
                .store
                .task_result(&task.agent_id)
                .map_err(map_store)?
                .is_some();
            let activity = self.scheduler.passive_activity_snapshot(&task.agent_id);
            let revision = activity
                .as_ref()
                .map(|activity| activity.revision)
                .unwrap_or(0)
                .max(task.last_event_seq);
            let terminal = task.phase == TaskPhase::Terminal;
            let now = Instant::now();
            if revision > query.after_revision
                || !pending_requests.is_empty()
                || terminal
                || now >= deadline
            {
                let timed_out = revision <= query.after_revision
                    && pending_requests.is_empty()
                    && !terminal
                    && now >= deadline;
                return Ok(RpcSuccess::TaskPoll {
                    activity: task_activity_view(
                        task.phase,
                        activity,
                        self.scheduler.diagnostic_tail(&task.agent_id),
                    ),
                    task: task_view(task.clone()),
                    revision,
                    next_revision: revision,
                    pending_requests,
                    command_pending_approval,
                    result_available,
                    latest_progress: self
                        .scheduler
                        .passive_activity_snapshot(&task.agent_id)
                        .and_then(|a| a.latest_progress),
                    // Result text is read through the bounded result endpoint;
                    // poll must remain queryable for arbitrarily large results.
                    result: None,
                    instruction: (!terminal).then(|| "Use poll for progress".to_owned()),
                    timed_out,
                });
            }
            thread::sleep((deadline - now).min(Duration::from_millis(10)));
        }
    }
}

fn result_page_bounds(
    text: &str,
    offset: usize,
    limit: usize,
) -> Result<(usize, Option<usize>), RpcError> {
    let total_bytes = text.len();
    if offset > total_bytes || !text.is_char_boundary(offset) {
        return Err(RpcError::new(
            RpcErrorCode::Validation,
            "result offset is outside the result",
        ));
    }
    let mut end = offset.saturating_add(limit).min(total_bytes);
    while end > offset && !text.is_char_boundary(end) {
        end -= 1;
    }
    if end == offset && offset < total_bytes {
        return Err(RpcError::new(
            RpcErrorCode::Validation,
            "result limit does not include a complete UTF-8 character",
        ));
    }
    let next_offset = (end < total_bytes).then_some(end);
    debug_assert!(next_offset.is_none_or(|next| next > offset));
    Ok((end, next_offset))
}

fn opaque_generation() -> Result<String, RpcServiceConfigError> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|_| RpcServiceConfigError::GenerationUnavailable)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn agent_capabilities() -> AgentCapabilitiesView {
    let maturity = BTreeMap::new();
    AgentCapabilitiesView {
        max_rpc_frame_bytes: MAX_FRAME_BYTES,
        max_wait_ms: MAX_WAIT.as_millis() as u64,
        maturity,
    }
}

fn parse_task_cursor(cursor: &str) -> Result<u64, RpcError> {
    let value = cursor
        .strip_prefix("task:")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| RpcError::new(RpcErrorCode::Validation, "task cursor is invalid"))?;
    Ok(value)
}

fn format_task_cursor(cursor: u64) -> String {
    format!("task:{cursor}")
}

fn task_view(task: TaskRecord) -> TaskView {
    TaskView {
        agent_id: task.agent_id,
        phase: match task.phase {
            TaskPhase::Queued => "QUEUED",
            TaskPhase::Preparing => "PREPARING",
            TaskPhase::Running => "RUNNING",
            TaskPhase::WaitingInput => "WAITING_INPUT",
            TaskPhase::Cancelling => "CANCELLING",
            TaskPhase::Terminal => "TERMINAL",
        }
        .into(),
        outcome: task.outcome,
        reason_code: task.failure_code,
        stop_requested: task.stop_requested,
        close_requested: task.close_requested,
        closed: task.closed_at.is_some(),
        reaped: task.reaped_at.is_some(),
    }
}

fn task_activity_view(
    phase: TaskPhase,
    snapshot: Option<PassiveActivitySnapshot>,
    diagnostic_tail: String,
) -> TaskActivityView {
    let state = match phase {
        TaskPhase::Queued => TaskActivityStateView::Queued,
        TaskPhase::Preparing => TaskActivityStateView::Preparing,
        TaskPhase::Running => TaskActivityStateView::Active,
        TaskPhase::WaitingInput => TaskActivityStateView::WaitingInput,
        TaskPhase::Cancelling => TaskActivityStateView::Cancelling,
        TaskPhase::Terminal => TaskActivityStateView::Terminal,
    };
    let Some(snapshot) = snapshot else {
        return TaskActivityView {
            state,
            last_runtime_event_at: None,
            last_activity_age_ms: None,
            model_request_active: false,
            model_request_age_ms: None,
            model_last_delta_age_ms: None,
            latest_text_tail: String::new(),
            diagnostic_tail,
            latest_text_updated_at: None,
            latest_text_truncated: false,
            latest_progress: None,
            active_tools: Vec::new(),
            window_60s: ActivityWindowView::default(),
            telemetry_status: TelemetryStatusView::Unavailable,
        };
    };
    TaskActivityView {
        state,
        last_runtime_event_at: snapshot.last_runtime_event_at,
        last_activity_age_ms: snapshot.last_activity_age_ms,
        model_request_active: snapshot.model_request_active,
        model_request_age_ms: snapshot.model_request_age_ms,
        model_last_delta_age_ms: snapshot.model_last_delta_age_ms,
        latest_text_tail: snapshot.latest_text_tail,
        diagnostic_tail,
        latest_text_updated_at: snapshot.latest_text_updated_at,
        latest_text_truncated: snapshot.latest_text_truncated,
        latest_progress: snapshot.latest_progress,
        active_tools: snapshot
            .active_tools
            .into_iter()
            .map(|tool| ActiveToolView {
                tool_call_id: tool.tool_call_id,
                kind: match tool.kind {
                    PassiveToolKind::Read => ActivityToolKindView::Read,
                    PassiveToolKind::Bash => ActivityToolKindView::Bash,
                    PassiveToolKind::Other => ActivityToolKindView::Other,
                },
            })
            .collect(),
        window_60s: activity_window_view(snapshot.window_60s),
        telemetry_status: if snapshot.telemetry_degraded {
            TelemetryStatusView::Degraded
        } else {
            TelemetryStatusView::Healthy
        },
    }
}

fn activity_window_view(value: PassiveActivityWindow) -> ActivityWindowView {
    ActivityWindowView {
        reasoning_delta_events: value.reasoning_delta_events,
        reasoning_delta_bytes: value.reasoning_delta_bytes,
        text_delta_events: value.text_delta_events,
        text_delta_bytes: value.text_delta_bytes,
        tool_calls_started: value.tool_calls_started,
        tool_calls_completed: value.tool_calls_completed,
        tool_calls_failed: value.tool_calls_failed,
        read_calls: value.read_calls,
        bash_calls: value.bash_calls,
        other_tool_calls: value.other_tool_calls,
    }
}

impl From<StoredTaskResult> for TaskResultView {
    fn from(stored: StoredTaskResult) -> Self {
        let total_bytes = stored.result.final_text.len();
        Self {
            outcome: stored.result.outcome,
            final_text: stored.result.final_text,
            partial: stored.result.partial,
            result_sha256: stored.result_sha256,
            offset: 0,
            total_bytes,
            next_offset: None,
            complete: true,
        }
    }
}

fn pending_request_view(request: StoredPendingRequest) -> PendingRequestView {
    let state = match request.state {
        PendingRequestState::Pending => PendingRequestStateView::Pending,
        PendingRequestState::Sending => PendingRequestStateView::Sending,
        PendingRequestState::Responded => PendingRequestStateView::Responded,
    };
    if request.request_type != "permission" {
        return PendingRequestView {
            request_id: request.request_id,
            kind: "unsupported_input".into(),
            state,
            respondable: false,
            tool_name: None,
            operation: "user_input".into(),
            summary: "unsupported user input request".into(),
            policy_preview: "unknown".into(),
        };
    }
    let params = serde_json::from_str::<Value>(&request.payload_json).ok();
    let tool_name = params
        .as_ref()
        .and_then(|value| value.get("toolName"))
        .and_then(Value::as_str)
        .map(|value| value.chars().take(64).collect::<String>());
    let operation = tool_name
        .as_deref()
        .map(operation_category)
        .unwrap_or("unknown")
        .to_owned();
    let summary = params
        .as_ref()
        .map(sanitized_permission_summary)
        .unwrap_or_else(|| "unrecognized permission request".into());
    let policy_preview = "official_permission_request".to_owned();
    PendingRequestView {
        request_id: request.request_id,
        kind: "permission".into(),
        state,
        respondable: true,
        tool_name,
        operation,
        summary,
        policy_preview,
    }
}

#[cfg(test)]
mod result_paging_tests {
    use super::{
        result_page_bounds, RpcResponse, RpcSuccess, TaskResultView, TaskView, MAX_FRAME_BYTES,
        MAX_RESULT_CHUNK_BYTES,
    };
    use zcode_agent_store::TaskOutcome;

    fn task() -> TaskView {
        TaskView {
            agent_id: "a".repeat(256),
            phase: "TERMINAL".into(),
            outcome: Some(TaskOutcome::Completed),
            reason_code: Some("r".repeat(256)),
            stop_requested: false,
            close_requested: false,
            closed: false,
            reaped: true,
        }
    }

    #[test]
    fn non_terminal_pages_always_advance() {
        assert_eq!(result_page_bounds("abcdef", 0, 3).unwrap(), (3, Some(3)));
        assert_eq!(result_page_bounds("abcdef", 3, 3).unwrap(), (6, None));
    }

    #[test]
    fn too_small_utf8_page_is_rejected_instead_of_stalling() {
        let error = result_page_bounds("你a", 0, 1).unwrap_err();
        assert_eq!(error.code, super::RpcErrorCode::Validation);
        assert_eq!(result_page_bounds("你a", 0, 3).unwrap(), (3, Some(3)));
        assert!(result_page_bounds("你a", 1, 3).is_err());
    }

    #[test]
    fn worst_case_encoded_result_response_and_newline_fit_the_frame() {
        let text = "\u{0}".repeat(MAX_RESULT_CHUNK_BYTES);
        let response = RpcResponse::success(
            "q".repeat(128),
            RpcSuccess::TaskResult {
                task: task(),
                result: Some(TaskResultView {
                    outcome: TaskOutcome::Completed,
                    final_text: text,
                    partial: false,
                    result_sha256: "f".repeat(64),
                    offset: 0,
                    total_bytes: MAX_RESULT_CHUNK_BYTES + 1,
                    next_offset: Some(MAX_RESULT_CHUNK_BYTES),
                    complete: false,
                }),
            },
        );
        assert!(serde_json::to_vec(&response).unwrap().len() + 1 <= MAX_FRAME_BYTES);
    }

    #[test]
    fn transport_projection_does_not_change_the_stored_outcome() {
        let view = TaskResultView {
            outcome: TaskOutcome::Failed,
            final_text: "failure".into(),
            partial: true,
            result_sha256: "f".repeat(64),
            offset: 0,
            total_bytes: 7,
            next_offset: None,
            complete: true,
        };
        assert_eq!(view.outcome, TaskOutcome::Failed);
        assert!(view.partial);
    }
}

fn operation_category(tool_name: &str) -> &'static str {
    match tool_name.to_ascii_lowercase().as_str() {
        "read" | "grep" | "glob" => "read",
        "write" | "edit" | "delete" | "move" => "write",
        "bash" | "execute" | "terminal" => "command",
        "network" => "network",
        "git_ref_mutation" => "git_ref_mutation",
        _ => "unknown",
    }
}

fn sanitized_permission_summary(params: &Value) -> String {
    let input = params.get("input").unwrap_or(&Value::Null);
    let leaf = |name: &str| {
        input
            .get(name)
            .and_then(Value::as_str)
            .and_then(|value| std::path::Path::new(value).file_name())
            .map(|value| value.to_string_lossy().chars().take(96).collect::<String>())
    };
    if let Some(program) = leaf("program") {
        let count = input
            .get("args")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        return format!("command {program} with {count} arguments");
    }
    if let Some(path) = leaf("path")
        .or_else(|| leaf("destination"))
        .or_else(|| leaf("source"))
    {
        return format!("target {path}");
    }
    if params
        .get("toolName")
        .and_then(Value::as_str)
        .is_some_and(|name| name.eq_ignore_ascii_case("git_ref_mutation"))
    {
        return "Git reference mutation".into();
    }
    "permission request".into()
}

fn validate_id(value: &str, field: &str) -> Result<(), RpcError> {
    validate_text(value, field, 256)
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_REQUEST_ID_BYTES && !value.contains('\0')
}

fn validate_text(value: &str, field: &str, max: usize) -> Result<(), RpcError> {
    if value.is_empty() || value.len() > max || value.contains('\0') {
        return Err(RpcError::new(
            RpcErrorCode::Validation,
            format!("{field} is invalid"),
        ));
    }
    Ok(())
}

fn map_scheduler(error: SchedulerError) -> RpcError {
    match error {
        SchedulerError::Store(error) => map_store(error),
        SchedulerError::InvalidConfig(message) => {
            // Preserve the bounded, actionable preparation reason. The MCP
            // facade may still redact it for callers, but RPC diagnostics
            // must distinguish repository, path, budget, and state errors.
            RpcError::new(
                RpcErrorCode::Validation,
                format!("scheduler rejected the operation: {message}"),
            )
        }
        SchedulerError::RuntimeSpawn { .. } | SchedulerError::LifecycleSink { .. } => {
            RpcError::new(RpcErrorCode::RuntimeLost, "runtime operation failed")
        }
        SchedulerError::RuntimeCommand { .. } => {
            RpcError::new(RpcErrorCode::Unavailable, "runtime command failed")
        }
    }
}

fn map_store(error: StoreError) -> RpcError {
    match error {
        StoreError::LegacySchemaUnsupported => RpcError::new(
            RpcErrorCode::Persistence,
            "STORE_SCHEMA_VERSION_UNSUPPORTED",
        ),
        StoreError::Sqlite(_) => {
            RpcError::new(RpcErrorCode::Persistence, "durable store operation failed")
        }
        StoreError::Conflict(message) if message.starts_with("WORKSPACE_BUSY") => {
            let active_agent_id = message
                .strip_prefix("WORKSPACE_BUSY active_agent_id=")
                .map(str::to_owned);
            let mut error = RpcError::new(RpcErrorCode::Conflict, "WORKSPACE_BUSY");
            error.active_agent_id = active_agent_id;
            error
        }
        StoreError::Conflict(_) => RpcError::new(RpcErrorCode::Conflict, "durable state conflict"),
        StoreError::InvalidState(_) => RpcError::new(
            RpcErrorCode::Validation,
            "durable state rejected the operation",
        ),
    }
}
