use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::Digest;
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const STORE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SCHEMA_VERSION: i64 = 11;

const SCHEMA: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE tasks (
    agent_id TEXT PRIMARY KEY,
    repository TEXT NOT NULL,
    phase TEXT NOT NULL,
    outcome TEXT,
    workspace_path TEXT NOT NULL,
    runtime_hash TEXT,
    prepared_launch_json TEXT NOT NULL,
    prepared_launch_sha256 TEXT NOT NULL,
    initial_prompt TEXT NOT NULL,
    zcode_session_id TEXT,
    turn_state TEXT NOT NULL DEFAULT 'IDLE',
    pid INTEGER,
    process_group_id INTEGER,
    process_uid INTEGER,
    process_start_token TEXT,
    runtime_agent_id TEXT,
    owner_id TEXT,
    owner_epoch INTEGER NOT NULL DEFAULT 0,
    lease_expires_at INTEGER,
    close_requested INTEGER NOT NULL DEFAULT 0,
    stop_requested INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    last_heartbeat_at INTEGER,
    last_event_seq INTEGER NOT NULL DEFAULT 0,
    failure_code TEXT,
    failure_message TEXT,
    closed_at INTEGER,
    reaped_at INTEGER,
    CHECK ((phase = 'TERMINAL') = (outcome IS NOT NULL))
);
CREATE INDEX tasks_queue_idx ON tasks(phase, created_at, agent_id);
CREATE INDEX tasks_workspace_phase_idx ON tasks(workspace_path, phase);
CREATE INDEX tasks_scope_idx ON tasks(repository, phase, created_at);

CREATE TABLE events (
    agent_id TEXT NOT NULL REFERENCES tasks(agent_id) ON DELETE CASCADE,
    runtime_agent_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    source_seq INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    turn_id TEXT,
    payload_json TEXT NOT NULL,
    redaction_level TEXT NOT NULL,
    PRIMARY KEY (agent_id, runtime_agent_id, seq),
    UNIQUE (agent_id, runtime_agent_id, source_seq)
);

CREATE TABLE messages (
    message_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES tasks(agent_id) ON DELETE CASCADE,
    mode TEXT NOT NULL,
    content TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    delivered_at INTEGER,
    target_turn_id TEXT,
    failure_code TEXT,
    failure_message TEXT
);

CREATE TABLE pending_requests (
    request_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES tasks(agent_id) ON DELETE CASCADE,
    correlation_id TEXT NOT NULL,
    request_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    responded_at INTEGER,
    response_decision TEXT,
    response_content TEXT,
    UNIQUE (agent_id, correlation_id)
);

CREATE TABLE task_results (
    agent_id TEXT PRIMARY KEY REFERENCES tasks(agent_id) ON DELETE CASCADE,
    outcome TEXT NOT NULL,
    final_text TEXT NOT NULL,
    partial INTEGER NOT NULL,
    result_sha256 TEXT NOT NULL,
    completed_at INTEGER NOT NULL
);

CREATE TABLE lifecycle_ledger (
    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES tasks(agent_id) ON DELETE CASCADE,
    owner_epoch INTEGER NOT NULL,
    from_phase TEXT,
    to_phase TEXT NOT NULL,
    outcome TEXT,
    reason_code TEXT,
    recorded_at INTEGER NOT NULL
);
"#;

#[derive(Debug)]
pub enum StoreError {
    Sqlite(rusqlite::Error),
    LegacySchemaUnsupported,
    InvalidState(String),
    Conflict(String),
}

impl StoreError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::LegacySchemaUnsupported => "STORE_SCHEMA_VERSION_UNSUPPORTED",
            Self::Sqlite(_) => "PERSISTENCE_ERROR",
            Self::InvalidState(_) => "INVALID_STATE",
            Self::Conflict(_) => "CONFLICT",
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "SQLite store error: {error}"),
            Self::LegacySchemaUnsupported => formatter.write_str(
                "STORE_SCHEMA_VERSION_UNSUPPORTED: existing Store must be backed up and recreated",
            ),
            Self::InvalidState(message) => write!(formatter, "invalid store state: {message}"),
            Self::Conflict(message) => write!(formatter, "store conflict: {message}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskPhase {
    Queued,
    Preparing,
    Running,
    WaitingInput,
    Cancelling,
    Terminal,
}

impl TaskPhase {
    pub fn is_terminal(self) -> bool {
        self == Self::Terminal
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Preparing => "PREPARING",
            Self::Running => "RUNNING",
            Self::WaitingInput => "WAITING_INPUT",
            Self::Cancelling => "CANCELLING",
            Self::Terminal => "TERMINAL",
        }
    }

    fn parse(value: &str) -> StoreResult<Self> {
        match value {
            "QUEUED" => Ok(Self::Queued),
            "PREPARING" => Ok(Self::Preparing),
            "RUNNING" => Ok(Self::Running),
            "WAITING_INPUT" => Ok(Self::WaitingInput),
            "CANCELLING" => Ok(Self::Cancelling),
            "TERMINAL" => Ok(Self::Terminal),
            other => Err(StoreError::InvalidState(format!(
                "unknown task phase {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskOutcome {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    RuntimeLost,
    ResultInvalid,
}

impl TaskOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::TimedOut => "TIMED_OUT",
            Self::RuntimeLost => "RUNTIME_LOST",
            Self::ResultInvalid => "RESULT_INVALID",
        }
    }

    fn parse(value: &str) -> StoreResult<Self> {
        match value {
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            "TIMED_OUT" => Ok(Self::TimedOut),
            "RUNTIME_LOST" => Ok(Self::RuntimeLost),
            "RESULT_INVALID" => Ok(Self::ResultInvalid),
            other => Err(StoreError::InvalidState(format!(
                "unknown task outcome {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTask {
    pub agent_id: String,
    pub repository: String,
    pub workspace_path: String,
    pub runtime_hash: Option<String>,
    pub prepared_launch_json: String,
    pub prepared_launch_sha256: String,
    pub initial_prompt: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub agent_id: String,
    pub repository: String,
    pub phase: TaskPhase,
    pub outcome: Option<TaskOutcome>,
    pub workspace_path: String,
    pub runtime_hash: Option<String>,
    pub prepared_launch_json: String,
    pub prepared_launch_sha256: String,
    pub initial_prompt: String,
    pub owner_id: Option<String>,
    pub owner_epoch: u64,
    pub close_requested: bool,
    pub stop_requested: bool,
    pub last_event_seq: u64,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub runtime_agent_id: Option<String>,
    pub zcode_session_id: Option<String>,
    pub turn_state: TurnState,
    pub process_identity: Option<StoredProcessIdentity>,
    pub closed_at: Option<i64>,
    pub reaped_at: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSubmissionDisposition {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmittedTask {
    pub task: TaskRecord,
    pub disposition: TaskSubmissionDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskResult {
    pub outcome: TaskOutcome,
    pub final_text: String,
    pub partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTaskResult {
    pub result: TaskResult,
    pub result_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskQueryScope<'a> {
    pub repository: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPageFilter {
    pub phase: Option<TaskPhase>,
    pub outcome: Option<TaskOutcome>,
}

/// Inclusive time/workspace selector used by the reduced adapter query API.
/// `start_ms` is compared with `created_at`; `end_ms` is compared with
/// `completed_at` and therefore only matches tasks that have completed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskWindowQuery {
    pub agent_id: Option<String>,
    pub workspace_path: Option<String>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPage {
    pub tasks: Vec<TaskRecord>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Idle,
    Active,
    Failed,
}

impl TurnState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "IDLE",
            Self::Active => "ACTIVE",
            Self::Failed => "FAILED",
        }
    }

    fn parse(value: &str) -> StoreResult<Self> {
        match value {
            "IDLE" => Ok(Self::Idle),
            "ACTIVE" => Ok(Self::Active),
            "FAILED" => Ok(Self::Failed),
            other => Err(StoreError::InvalidState(format!(
                "unknown turn state {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProcessIdentity {
    pub pid: u32,
    pub process_group_id: i32,
    pub uid: u32,
    pub start_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskClaim {
    pub task: TaskRecord,
    pub owner_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleWrite {
    pub agent_id: String,
    pub runtime_agent_id: String,
    pub owner_epoch: u64,
    pub source_sequence: u64,
    pub event_type: String,
    pub turn_id: Option<String>,
    pub payload_json: String,
    pub redaction_level: String,
    pub terminal: Option<TerminalUpdate>,
    pub turn_state: Option<TurnState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalUpdate {
    pub outcome: TaskOutcome,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlDecision {
    pub phase: TaskPhase,
    pub outcome: Option<TaskOutcome>,
    pub owner_epoch: u64,
    pub needs_runtime_stop: bool,
    pub prior_stop_or_close: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    Queued,
    Sending,
    Delivered,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessage {
    pub message_id: String,
    pub agent_id: String,
    pub mode: String,
    pub content: String,
    pub state: MessageState,
    pub target_turn_id: Option<String>,
    pub failure_code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingRequestState {
    Pending,
    Sending,
    Responded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPendingRequest {
    pub request_id: String,
    pub agent_id: String,
    pub correlation_id: String,
    pub request_type: String,
    pub payload_json: String,
    pub state: PendingRequestState,
    pub response_decision: Option<String>,
    pub response_content: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingResponseClaimDisposition {
    Claimed,
    TaskStopping,
    NotPending(PendingRequestState),
    NotFound,
}

pub struct Store {
    connection: Mutex<Connection>,
    database_path: PathBuf,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        let mut connection = Connection::open(path.as_ref())?;
        connection.busy_timeout(STORE_BUSY_TIMEOUT)?;
        initialize_schema(&mut connection)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        let database_path = std::fs::canonicalize(path.as_ref()).map_err(|error| {
            StoreError::InvalidState(format!("database path cannot be canonicalized: {error}"))
        })?;
        Ok(Self {
            connection: Mutex::new(connection),
            database_path,
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn journal_mode(&self) -> StoreResult<String> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?)
    }

    pub fn enqueue_task_authoritative(&self, task: &NewTask) -> StoreResult<SubmittedTask> {
        validate_task(task)?;
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if query_task(&transaction, &task.agent_id)?.is_some() {
            return Err(StoreError::Conflict(format!(
                "agent id {} already exists",
                task.agent_id
            )));
        }
        // Workspace ownership is the collision boundary.  It applies to every
        // submission, regardless of launch metadata or repository identity;
        // terminal rows remain queryable and therefore do not block reuse.
        if let Some(active_agent_id) = transaction
            .query_row(
                "SELECT agent_id FROM tasks WHERE workspace_path=?1
             AND phase IN ('QUEUED','PREPARING','RUNNING','WAITING_INPUT','CANCELLING')
             ORDER BY created_at, rowid LIMIT 1",
                [&task.workspace_path],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return Err(StoreError::Conflict(format!(
                "WORKSPACE_BUSY active_agent_id={active_agent_id}"
            )));
        }
        let created_at = now_millis();
        transaction.execute(
            "INSERT INTO tasks (
                agent_id,repository,phase,workspace_path,runtime_hash,prepared_launch_json,
                prepared_launch_sha256,initial_prompt,created_at
             ) VALUES (?1,?2,'QUEUED',?3,?4,?5,?6,?7,?8)",
            params![
                task.agent_id,
                task.repository,
                task.workspace_path,
                task.runtime_hash,
                task.prepared_launch_json,
                task.prepared_launch_sha256,
                task.initial_prompt,
                created_at,
            ],
        )?;
        insert_ledger(
            &transaction,
            &task.agent_id,
            0,
            None,
            TaskPhase::Queued,
            None,
            None,
        )?;
        let stored = query_task(&transaction, &task.agent_id)?
            .ok_or_else(|| StoreError::InvalidState("inserted task disappeared".into()))?;
        transaction.commit()?;
        Ok(SubmittedTask {
            task: stored,
            disposition: TaskSubmissionDisposition::Created,
        })
    }

    pub fn get_task(&self, agent_id: &str) -> StoreResult<Option<TaskRecord>> {
        let connection = self.connection.lock().unwrap();
        query_task(&connection, agent_id)
    }

    pub fn get_task_scoped(
        &self,
        agent_id: &str,
        scope: TaskQueryScope<'_>,
    ) -> StoreResult<Option<TaskRecord>> {
        validate_scope(&scope)?;
        let connection = self.connection.lock().unwrap();
        let found = connection
            .query_row(
                "SELECT agent_id FROM tasks
                 WHERE agent_id=?1
                   AND (?2 IS NULL OR repository=?2)
                   ",
                params![agent_id, scope.repository],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        found
            .map(|id| query_task(&connection, &id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn list_task_page(
        &self,
        scope: TaskQueryScope<'_>,
        filter: TaskPageFilter,
        cursor: Option<u64>,
        limit: usize,
    ) -> StoreResult<TaskPage> {
        validate_scope(&scope)?;
        if limit == 0 {
            return Err(StoreError::InvalidState(
                "task page limit must be positive".into(),
            ));
        }
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT rowid,agent_id FROM tasks
             WHERE (?1 IS NULL OR repository=?1)
               AND (?2 IS NULL OR phase=?2)
               AND (?3 IS NULL OR outcome=?3)
               AND (?4 IS NULL OR rowid < ?4)
             ORDER BY rowid DESC LIMIT ?5",
        )?;
        let rows = statement
            .query_map(
                params![
                    scope.repository,
                    filter.phase.map(TaskPhase::as_str),
                    filter.outcome.map(TaskOutcome::as_str),
                    cursor.map(u64_to_i64).transpose()?,
                    usize_to_i64(limit.saturating_add(1))?,
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let has_more = rows.len() > limit;
        let selected = rows.into_iter().take(limit).collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| selected.last().map(|(rowid, _)| i64_to_u64(*rowid)))
            .flatten()
            .transpose()?;
        let tasks = selected
            .into_iter()
            .map(|(_, id)| {
                query_task(&connection, &id)?
                    .ok_or_else(|| StoreError::InvalidState("listed task disappeared".into()))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(TaskPage { tasks, next_cursor })
    }

    pub fn list_task_window(&self, query: &TaskWindowQuery) -> StoreResult<Vec<TaskRecord>> {
        if query.agent_id.is_none() && query.workspace_path.is_none() {
            return Err(StoreError::InvalidState(
                "agent_id or workspace_path query scope is required".into(),
            ));
        }
        if let (Some(start), Some(end)) = (query.start_ms, query.end_ms) {
            if start > end {
                return Err(StoreError::InvalidState(
                    "query start_ms must be less than or equal to end_ms".into(),
                ));
            }
        }
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT agent_id FROM tasks
             WHERE (?1 IS NULL OR agent_id=?1)
               AND (?2 IS NULL OR workspace_path=?2)
               AND (?3 IS NULL OR created_at>=?3)
               AND (?4 IS NULL OR (completed_at IS NOT NULL AND completed_at<=?4))
             ORDER BY created_at, agent_id",
        )?;
        let ids = statement
            .query_map(
                params![
                    query.agent_id,
                    query.workspace_path,
                    query.start_ms,
                    query.end_ms
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                query_task(&connection, &id)?
                    .ok_or_else(|| StoreError::InvalidState("task disappeared during query".into()))
            })
            .collect()
    }

    pub fn claim_next(
        &self,
        owner_id: &str,
        global_limit: usize,
        per_workspace_limit: usize,
    ) -> StoreResult<Option<TaskClaim>> {
        // The product contract deliberately has no process-wide agent cap.
        // Keep the parameter for source compatibility with older daemon
        // callers, but do not use it as an admission gate.  Concurrency is
        // scoped to the canonical repository/workspace below.
        let _ = global_limit;
        if per_workspace_limit == 0 {
            return Ok(None);
        }
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let candidate = transaction
            .query_row(
                "SELECT agent_id FROM tasks queued
                 WHERE phase='QUEUED' AND close_requested=0
                   AND (SELECT COUNT(*) FROM tasks active
                        WHERE active.repository=queued.repository
                          AND active.phase IN ('PREPARING','RUNNING','WAITING_INPUT','CANCELLING')) < ?1
                 ORDER BY queued.created_at,queued.rowid LIMIT 1",
                [usize_to_i64(per_workspace_limit)?],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(agent_id) = candidate else {
            transaction.commit()?;
            return Ok(None);
        };
        let now = now_millis();
        let changed = transaction.execute(
            "UPDATE tasks SET phase='PREPARING',owner_id=?1,owner_epoch=owner_epoch+1,
                 started_at=COALESCE(started_at,?2),last_heartbeat_at=?2
             WHERE agent_id=?3 AND phase='QUEUED' AND close_requested=0",
            params![owner_id, now, agent_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "task {agent_id} lost its queue claim"
            )));
        }
        let task = query_task(&transaction, &agent_id)?
            .ok_or_else(|| StoreError::InvalidState("claimed task disappeared".into()))?;
        insert_ledger(
            &transaction,
            &agent_id,
            task.owner_epoch,
            Some(TaskPhase::Queued),
            TaskPhase::Preparing,
            None,
            Some("CLAIMED"),
        )?;
        transaction.commit()?;
        Ok(Some(TaskClaim {
            owner_epoch: task.owner_epoch,
            task,
        }))
    }

    pub fn mark_session_running(
        &self,
        agent_id: &str,
        owner_epoch: u64,
        runtime_agent_id: &str,
        identity: Option<&StoredProcessIdentity>,
        zcode_session_id: Option<&str>,
        turn_state: Option<TurnState>,
    ) -> StoreResult<bool> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE tasks SET phase='RUNNING',runtime_agent_id=?1,pid=?2,process_group_id=?3,
                 process_uid=?4,process_start_token=?5,last_heartbeat_at=?6,
                 zcode_session_id=COALESCE(?7,zcode_session_id),turn_state=COALESCE(?8,turn_state)
             WHERE agent_id=?9 AND owner_epoch=?10 AND phase='PREPARING'",
            params![
                runtime_agent_id,
                identity.map(|value| value.pid),
                identity.map(|value| value.process_group_id),
                identity.map(|value| value.uid),
                identity.map(|value| value.start_token.as_str()),
                now_millis(),
                zcode_session_id,
                turn_state.map(TurnState::as_str),
                agent_id,
                u64_to_i64(owner_epoch)?,
            ],
        )?;
        if changed == 1 {
            insert_ledger(
                &transaction,
                agent_id,
                owner_epoch,
                Some(TaskPhase::Preparing),
                TaskPhase::Running,
                None,
                Some("RUNTIME_STARTED"),
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn append_lifecycle(&self, write: &LifecycleWrite) -> StoreResult<u64> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT seq FROM events WHERE agent_id=?1 AND runtime_agent_id=?2 AND source_seq=?3",
                params![
                    write.agent_id,
                    write.runtime_agent_id,
                    u64_to_i64(write.source_sequence)?
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            transaction.commit()?;
            return i64_to_u64(existing);
        }
        let (phase, outcome, epoch, close_requested, stop_requested) =
            query_guard(&transaction, &write.agent_id)?;
        if phase == TaskPhase::Terminal || epoch != write.owner_epoch {
            return Err(StoreError::Conflict(format!(
                "late lifecycle record rejected for {} epoch {}",
                write.agent_id, write.owner_epoch
            )));
        }
        debug_assert!(outcome.is_none());
        let last_seq: i64 = transaction.query_row(
            "SELECT last_event_seq FROM tasks WHERE agent_id=?1",
            [&write.agent_id],
            |row| row.get(0),
        )?;
        let sequence = last_seq
            .checked_add(1)
            .ok_or_else(|| StoreError::InvalidState("event sequence overflow".into()))?;
        transaction.execute(
            "INSERT INTO events(agent_id,runtime_agent_id,seq,source_seq,timestamp,event_type,
                 turn_id,payload_json,redaction_level) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                write.agent_id,
                write.runtime_agent_id,
                sequence,
                u64_to_i64(write.source_sequence)?,
                now_millis(),
                write.event_type,
                write.turn_id,
                write.payload_json,
                write.redaction_level,
            ],
        )?;
        transaction.execute(
            "UPDATE tasks SET last_event_seq=?1,last_heartbeat_at=?2,
                 turn_state=COALESCE(?3,turn_state) WHERE agent_id=?4",
            params![
                sequence,
                now_millis(),
                write.turn_state.map(TurnState::as_str),
                write.agent_id,
            ],
        )?;
        if let Some(terminal) = &write.terminal {
            apply_terminal(
                &transaction,
                &write.agent_id,
                write.owner_epoch,
                phase,
                close_requested,
                stop_requested,
                terminal,
            )?;
        }
        transaction.commit()?;
        i64_to_u64(sequence)
    }

    pub fn transition_terminal(
        &self,
        agent_id: &str,
        owner_epoch: u64,
        terminal: &TerminalUpdate,
    ) -> StoreResult<TaskOutcome> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, outcome, epoch, close_requested, stop_requested) =
            query_guard(&transaction, agent_id)?;
        if phase == TaskPhase::Terminal {
            transaction.commit()?;
            return outcome.ok_or_else(|| {
                StoreError::InvalidState("terminal task is missing its outcome".into())
            });
        }
        if epoch != owner_epoch {
            return Err(StoreError::Conflict(format!(
                "owner epoch changed for {agent_id}"
            )));
        }
        let outcome = apply_terminal(
            &transaction,
            agent_id,
            owner_epoch,
            phase,
            close_requested,
            stop_requested,
            terminal,
        )?;
        transaction.commit()?;
        Ok(outcome)
    }

    pub fn fail_claim(
        &self,
        agent_id: &str,
        owner_epoch: u64,
        failure_code: &str,
        message: &str,
    ) -> StoreResult<TaskOutcome> {
        self.transition_terminal(
            agent_id,
            owner_epoch,
            &TerminalUpdate {
                outcome: TaskOutcome::RuntimeLost,
                failure_code: Some(failure_code.into()),
                failure_message: Some(message.into()),
            },
        )
    }

    pub fn request_close(&self, agent_id: &str) -> StoreResult<ControlDecision> {
        self.request_stop_internal(agent_id, true, true)
    }

    pub fn request_stop(&self, agent_id: &str) -> StoreResult<ControlDecision> {
        self.request_stop_internal(agent_id, false, true)
    }

    pub fn request_runtime_stop(&self, agent_id: &str) -> StoreResult<ControlDecision> {
        self.request_stop_internal(agent_id, false, false)
    }

    fn request_stop_internal(
        &self,
        agent_id: &str,
        close: bool,
        cancellation_intent: bool,
    ) -> StoreResult<ControlDecision> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, outcome, epoch, close_requested, stop_requested) =
            query_guard(&transaction, agent_id)?;
        let needs_runtime_stop = matches!(
            phase,
            TaskPhase::Preparing
                | TaskPhase::Running
                | TaskPhase::WaitingInput
                | TaskPhase::Cancelling
        );
        let next_phase = if phase == TaskPhase::Terminal {
            phase
        } else {
            TaskPhase::Cancelling
        };
        transaction.execute(
            "UPDATE tasks SET phase=?1,
                 close_requested=CASE WHEN ?2=1 THEN 1 ELSE close_requested END,
                 stop_requested=CASE WHEN ?3=1 THEN 1 ELSE stop_requested END,
                 closed_at=CASE WHEN ?2=1 THEN COALESCE(closed_at,?4) ELSE closed_at END
             WHERE agent_id=?5",
            params![
                next_phase.as_str(),
                close,
                cancellation_intent,
                now_millis(),
                agent_id,
            ],
        )?;
        if phase != TaskPhase::Terminal {
            settle_terminal_commands(&transaction, agent_id, "STOP_REQUESTED")?;
            if phase != next_phase {
                insert_ledger(
                    &transaction,
                    agent_id,
                    epoch,
                    Some(phase),
                    next_phase,
                    None,
                    Some(if close {
                        "CLOSE_REQUESTED"
                    } else {
                        "STOP_REQUESTED"
                    }),
                )?;
            }
        }
        transaction.commit()?;
        Ok(ControlDecision {
            phase: next_phase,
            outcome,
            owner_epoch: epoch,
            needs_runtime_stop,
            prior_stop_or_close: close_requested || stop_requested,
        })
    }

    pub fn requeue_task_for_resume_with_message(
        &self,
        agent_id: &str,
        message_id: &str,
        content: &str,
    ) -> StoreResult<()> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, session_id, workspace_path, owner_epoch): (
            TaskPhase,
            Option<String>,
            String,
            i64,
        ) = transaction.query_row(
            "SELECT phase,zcode_session_id,workspace_path,owner_epoch
             FROM tasks WHERE agent_id=?1",
            [agent_id],
            |row| {
                Ok((
                    TaskPhase::parse(&row.get::<_, String>(0)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                ))
            },
        )?;
        if phase != TaskPhase::Terminal {
            return Err(StoreError::Conflict(format!(
                "task {agent_id} is not terminal"
            )));
        }
        if session_id.is_none() {
            return Err(StoreError::InvalidState(
                "task has no persisted session id to resume".into(),
            ));
        }
        if let Some(active_agent_id) = transaction
            .query_row(
                "SELECT agent_id FROM tasks
                 WHERE workspace_path=?1 AND agent_id!=?2
                   AND phase IN ('QUEUED','PREPARING','RUNNING','WAITING_INPUT','CANCELLING')
                 ORDER BY created_at,rowid LIMIT 1",
                params![workspace_path, agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return Err(StoreError::Conflict(format!(
                "WORKSPACE_BUSY active_agent_id={active_agent_id}"
            )));
        }
        if transaction
            .query_row(
                "SELECT 1 FROM messages WHERE message_id=?1",
                [message_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(StoreError::Conflict(format!(
                "message {message_id} already exists"
            )));
        }
        transaction.execute("DELETE FROM task_results WHERE agent_id=?1", [agent_id])?;
        let changed = transaction.execute(
            "UPDATE tasks SET phase='QUEUED',outcome=NULL,owner_id=NULL,
                 lease_expires_at=NULL,close_requested=0,stop_requested=0,
                 failure_code=NULL,failure_message=NULL,closed_at=NULL,reaped_at=NULL,
                 completed_at=NULL,last_event_seq=0,turn_state='IDLE',pid=NULL,
                 process_group_id=NULL,process_uid=NULL,process_start_token=NULL,
                 runtime_agent_id=NULL WHERE agent_id=?1 AND phase='TERMINAL'",
            [agent_id],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "task {agent_id} changed before resume"
            )));
        }
        insert_ledger(
            &transaction,
            agent_id,
            i64_to_u64(owner_epoch)?,
            Some(TaskPhase::Terminal),
            TaskPhase::Queued,
            None,
            Some("RESUME_REQUESTED"),
        )?;
        transaction.execute(
            "INSERT INTO messages(message_id,agent_id,mode,content,state,created_at)
             VALUES (?1,?2,'queue',?3,'QUEUED',?4)",
            params![message_id, agent_id, content, now_millis()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn reap_task(&self, agent_id: &str) -> StoreResult<TaskOutcome> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, outcome, _, _, _) = query_guard(&transaction, agent_id)?;
        if phase != TaskPhase::Terminal {
            return Err(StoreError::InvalidState(format!(
                "cannot reap nonterminal task {agent_id}"
            )));
        }
        transaction.execute(
            "UPDATE tasks SET owner_id=NULL,lease_expires_at=NULL,
                 reaped_at=COALESCE(reaped_at,?1) WHERE agent_id=?2",
            params![now_millis(), agent_id],
        )?;
        transaction.commit()?;
        outcome.ok_or_else(|| StoreError::InvalidState("terminal task has no outcome".into()))
    }

    pub fn restore_terminal_after_resume_failure(
        &self,
        original: &TaskRecord,
        result: Option<&TaskResult>,
    ) -> StoreResult<()> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE tasks SET phase='TERMINAL',outcome=?1,owner_id=?2,lease_expires_at=NULL,
                 close_requested=?3,stop_requested=?4,failure_code=?5,failure_message=?6,
                 runtime_agent_id=?7,zcode_session_id=?8,turn_state=?9,pid=?10,
                 process_group_id=?11,process_uid=?12,process_start_token=?13,
                 closed_at=?14,reaped_at=?15,last_event_seq=?16
             WHERE agent_id=?17",
            params![
                original.outcome.map(TaskOutcome::as_str),
                original.owner_id,
                original.close_requested,
                original.stop_requested,
                original.failure_code,
                original.failure_message,
                original.runtime_agent_id,
                original.zcode_session_id,
                original.turn_state.as_str(),
                original.process_identity.as_ref().map(|v| v.pid),
                original
                    .process_identity
                    .as_ref()
                    .map(|v| v.process_group_id),
                original.process_identity.as_ref().map(|v| v.uid),
                original.process_identity.as_ref().map(|v| &v.start_token),
                original.closed_at,
                original.reaped_at,
                u64_to_i64(original.last_event_seq)?,
                original.agent_id,
            ],
        )?;
        if let Some(result) = result {
            transaction.execute(
                "DELETE FROM task_results WHERE agent_id=?1",
                [original.agent_id.as_str()],
            )?;
            let canonical = task_result_bytes(result)?;
            let digest = task_result_digest(&canonical);
            transaction.execute(
                "INSERT INTO task_results(agent_id,outcome,final_text,partial,result_sha256,completed_at)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    original.agent_id,
                    result.outcome.as_str(),
                    result.final_text,
                    result.partial,
                    digest,
                    now_millis(),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn startup_recovery_tasks(&self) -> StoreResult<Vec<TaskRecord>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT agent_id FROM tasks
             WHERE phase IN ('PREPARING','RUNNING','WAITING_INPUT','CANCELLING')
                OR (phase='TERMINAL' AND reaped_at IS NULL)
             ORDER BY created_at,rowid",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|agent_id| {
                query_task(&connection, &agent_id)?.ok_or_else(|| {
                    StoreError::InvalidState("startup recovery task disappeared".into())
                })
            })
            .collect()
    }

    pub fn store_task_result(&self, agent_id: &str, result: &TaskResult) -> StoreResult<()> {
        validate_result(result)?;
        let canonical = task_result_bytes(result)?;
        let digest = task_result_digest(&canonical);
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = query_task(&transaction, agent_id)?
            .ok_or_else(|| StoreError::InvalidState(format!("unknown task {agent_id}")))?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT result_sha256 FROM task_results WHERE agent_id=?1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            if existing == digest {
                transaction.commit()?;
                return Ok(());
            }
            return Err(StoreError::Conflict(format!(
                "task {agent_id} already has a different immutable result"
            )));
        }
        if task.phase == TaskPhase::Terminal {
            return Err(StoreError::Conflict(format!(
                "task {agent_id} is terminal without this result"
            )));
        }
        if !matches!(
            task.phase,
            TaskPhase::Preparing
                | TaskPhase::Running
                | TaskPhase::WaitingInput
                | TaskPhase::Cancelling
        ) {
            return Err(StoreError::Conflict(format!(
                "task {agent_id} cannot complete from {:?}",
                task.phase
            )));
        }
        if result.outcome == TaskOutcome::Completed {
            let (pending, queued) = completion_blockers_tx(&transaction, agent_id)?;
            if pending || queued {
                return Err(StoreError::Conflict(
                    "task completion is blocked by pending input or queued messages".into(),
                ));
            }
        }
        if (task.stop_requested || task.close_requested) && result.outcome != TaskOutcome::Cancelled
        {
            return Err(StoreError::Conflict(
                "cancellation or close intent wins over late result".into(),
            ));
        }
        transaction.execute(
            "INSERT INTO task_results(agent_id,outcome,final_text,partial,result_sha256,completed_at)
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                agent_id,
                result.outcome.as_str(),
                result.final_text,
                result.partial,
                digest,
                now_millis(),
            ],
        )?;
        apply_terminal(
            &transaction,
            agent_id,
            task.owner_epoch,
            task.phase,
            task.close_requested,
            task.stop_requested,
            &TerminalUpdate {
                outcome: result.outcome,
                failure_code: task.failure_code.or_else(|| {
                    (result.outcome != TaskOutcome::Completed).then(|| "task failed".to_string())
                }),
                failure_message: task.failure_message,
            },
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn task_result(&self, agent_id: &str) -> StoreResult<Option<StoredTaskResult>> {
        let connection = self.connection.lock().unwrap();
        query_task_result(&connection, agent_id)
    }

    pub fn insert_message(
        &self,
        message_id: &str,
        agent_id: &str,
        mode: &str,
        content: &str,
    ) -> StoreResult<bool> {
        if mode != "queue" {
            return Err(StoreError::InvalidState(
                "only queue message mode is supported".into(),
            ));
        }
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, _, _, close_requested, stop_requested) = query_guard(&transaction, agent_id)?;
        if phase == TaskPhase::Terminal || close_requested || stop_requested {
            return Err(StoreError::Conflict(
                "terminal or stopping task cannot accept messages".into(),
            ));
        }
        let changed = transaction.execute(
            "INSERT OR IGNORE INTO messages(message_id,agent_id,mode,content,state,created_at)
             VALUES (?1,?2,?3,?4,'QUEUED',?5)",
            params![message_id, agent_id, mode, content, now_millis()],
        )?;
        if changed == 0 {
            let existing = query_message(&transaction, message_id)?.ok_or_else(|| {
                StoreError::InvalidState("message idempotency row disappeared".into())
            })?;
            if existing.agent_id != agent_id || existing.mode != mode || existing.content != content
            {
                return Err(StoreError::Conflict(
                    "message id is already bound to different content".into(),
                ));
            }
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn message(&self, message_id: &str) -> StoreResult<Option<StoredMessage>> {
        let connection = self.connection.lock().unwrap();
        query_message(&connection, message_id)
    }

    pub fn claim_next_message(&self, agent_id: &str) -> StoreResult<Option<StoredMessage>> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, _, _, close_requested, stop_requested) = query_guard(&transaction, agent_id)?;
        if phase != TaskPhase::Running || close_requested || stop_requested {
            transaction.commit()?;
            return Ok(None);
        }
        let id = transaction
            .query_row(
                "SELECT message_id FROM messages WHERE agent_id=?1 AND state='QUEUED'
                 ORDER BY created_at,rowid LIMIT 1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(id) = id else {
            transaction.commit()?;
            return Ok(None);
        };
        transaction.execute(
            "UPDATE messages SET state='SENDING' WHERE message_id=?1 AND state='QUEUED'",
            [&id],
        )?;
        let message = query_message(&transaction, &id)?;
        transaction.commit()?;
        Ok(message)
    }

    pub fn complete_message(
        &self,
        message_id: &str,
        target_turn_id: Option<&str>,
    ) -> StoreResult<bool> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.execute(
            "UPDATE messages SET state='DELIVERED',delivered_at=?1,target_turn_id=?2
             WHERE message_id=?3 AND state='SENDING'",
            params![now_millis(), target_turn_id, message_id],
        )? == 1)
    }

    pub fn fail_message(&self, message_id: &str, code: &str, message: &str) -> StoreResult<bool> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.execute(
            "UPDATE messages SET state='FAILED',failure_code=?1,failure_message=?2
             WHERE message_id=?3 AND state IN ('QUEUED','SENDING')",
            params![code, message, message_id],
        )? == 1)
    }

    pub fn insert_pending_request(
        &self,
        request_id: &str,
        agent_id: &str,
        correlation_id: &str,
        request_type: &str,
        payload_json: &str,
    ) -> StoreResult<bool> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (phase, _, owner_epoch, close_requested, stop_requested) =
            query_guard(&transaction, agent_id)?;
        if !matches!(phase, TaskPhase::Running | TaskPhase::WaitingInput)
            || close_requested
            || stop_requested
        {
            return Err(StoreError::Conflict(
                "task cannot publish a pending request in its current phase".into(),
            ));
        }
        let changed = transaction.execute(
            "INSERT OR IGNORE INTO pending_requests(request_id,agent_id,correlation_id,
                 request_type,payload_json,state,created_at)
             VALUES (?1,?2,?3,?4,?5,'PENDING',?6)",
            params![
                request_id,
                agent_id,
                correlation_id,
                request_type,
                payload_json,
                now_millis()
            ],
        )?;
        if changed == 0 {
            let existing = query_pending_request(&transaction, request_id)?.ok_or_else(|| {
                StoreError::InvalidState("pending request idempotency row disappeared".into())
            })?;
            if existing.agent_id != agent_id
                || existing.correlation_id != correlation_id
                || existing.request_type != request_type
                || existing.payload_json != payload_json
            {
                return Err(StoreError::Conflict(
                    "pending request identity has different content".into(),
                ));
            }
        }
        if changed == 1 && phase == TaskPhase::Running {
            transaction.execute(
                "UPDATE tasks SET phase='WAITING_INPUT' WHERE agent_id=?1 AND phase='RUNNING'",
                [agent_id],
            )?;
            insert_ledger(
                &transaction,
                agent_id,
                owner_epoch,
                Some(TaskPhase::Running),
                TaskPhase::WaitingInput,
                None,
                Some("INPUT_REQUESTED"),
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    pub fn pending_request(
        &self,
        agent_id: &str,
        request_id: &str,
    ) -> StoreResult<Option<StoredPendingRequest>> {
        let connection = self.connection.lock().unwrap();
        let request = query_pending_request(&connection, request_id)?;
        Ok(request.filter(|request| request.agent_id == agent_id))
    }

    pub fn pending_requests(&self, agent_id: &str) -> StoreResult<Vec<StoredPendingRequest>> {
        self.pending_requests_bounded(agent_id, i64::MAX as usize)
    }

    pub fn pending_requests_bounded(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<StoredPendingRequest>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT request_id FROM pending_requests
             WHERE agent_id=?1 AND state IN ('PENDING','SENDING')
             ORDER BY created_at,rowid LIMIT ?2",
        )?;
        let ids = statement
            .query_map(params![agent_id, usize_to_i64(limit)?], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                query_pending_request(&connection, &id)?
                    .ok_or_else(|| StoreError::InvalidState("pending request disappeared".into()))
            })
            .collect()
    }

    pub fn completion_blockers(&self, agent_id: &str) -> StoreResult<(bool, bool)> {
        let connection = self.connection.lock().unwrap();
        completion_blockers_tx(&connection, agent_id)
    }

    pub fn claim_pending_response_if_accepting(
        &self,
        agent_id: &str,
        request_id: &str,
        decision: &str,
        content: Option<&str>,
    ) -> StoreResult<PendingResponseClaimDisposition> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(request) = query_pending_request(&transaction, request_id)? else {
            transaction.commit()?;
            return Ok(PendingResponseClaimDisposition::NotFound);
        };
        if request.agent_id != agent_id {
            transaction.commit()?;
            return Ok(PendingResponseClaimDisposition::NotFound);
        }
        if request.state != PendingRequestState::Pending {
            transaction.commit()?;
            return Ok(PendingResponseClaimDisposition::NotPending(request.state));
        }
        let (phase, _, _, close_requested, stop_requested) = query_guard(&transaction, agent_id)?;
        if !matches!(phase, TaskPhase::Running | TaskPhase::WaitingInput)
            || close_requested
            || stop_requested
        {
            transaction.commit()?;
            return Ok(PendingResponseClaimDisposition::TaskStopping);
        }
        transaction.execute(
            "UPDATE pending_requests SET state='SENDING',response_decision=?1,response_content=?2
             WHERE request_id=?3 AND agent_id=?4 AND state='PENDING'",
            params![decision, content, request_id, agent_id],
        )?;
        transaction.commit()?;
        Ok(PendingResponseClaimDisposition::Claimed)
    }

    pub fn complete_pending_response(&self, agent_id: &str, request_id: &str) -> StoreResult<bool> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE pending_requests SET state='RESPONDED',responded_at=?1
             WHERE request_id=?2 AND agent_id=?3 AND state='SENDING'",
            params![now_millis(), request_id, agent_id],
        )? == 1;
        if changed {
            let remaining: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM pending_requests
                 WHERE agent_id=?1 AND state IN ('PENDING','SENDING'))",
                [agent_id],
                |row| row.get(0),
            )?;
            let (phase, _, owner_epoch, close_requested, stop_requested) =
                query_guard(&transaction, agent_id)?;
            if !remaining && phase == TaskPhase::WaitingInput && !close_requested && !stop_requested
            {
                transaction.execute(
                    "UPDATE tasks SET phase='RUNNING'
                     WHERE agent_id=?1 AND phase='WAITING_INPUT'",
                    [agent_id],
                )?;
                insert_ledger(
                    &transaction,
                    agent_id,
                    owner_epoch,
                    Some(TaskPhase::WaitingInput),
                    TaskPhase::Running,
                    None,
                    Some("INPUT_RESOLVED"),
                )?;
            }
        }
        transaction.commit()?;
        Ok(changed)
    }

    pub fn release_pending_response(&self, agent_id: &str, request_id: &str) -> StoreResult<bool> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.execute(
            "UPDATE pending_requests SET state='PENDING',response_decision=NULL,response_content=NULL
             WHERE request_id=?1 AND agent_id=?2 AND state='SENDING'",
            params![request_id, agent_id],
        )? == 1)
    }

    pub fn active_count(&self) -> StoreResult<u64> {
        let connection = self.connection.lock().unwrap();
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM tasks WHERE phase IN ('PREPARING','RUNNING','WAITING_INPUT','CANCELLING')",
            [],
            |row| row.get(0),
        )?;
        i64_to_u64(count)
    }
}

fn initialize_schema(connection: &mut Connection) -> StoreResult<()> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let user_tables: u64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if user_tables != 0 {
        if version != SCHEMA_VERSION || !schema_is_current(connection)? {
            return Err(StoreError::LegacySchemaUnsupported);
        }
        return Ok(());
    }
    if version != 0 {
        return Err(StoreError::LegacySchemaUnsupported);
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn schema_is_current(connection: &Connection) -> StoreResult<bool> {
    let expected = [
        "events",
        "lifecycle_ledger",
        "messages",
        "pending_requests",
        "task_results",
        "tasks",
    ];
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let actual = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(actual == expected)
}

fn validate_task(task: &NewTask) -> StoreResult<()> {
    for (name, value) in [
        ("agent_id", task.agent_id.as_str()),
        ("repository", task.repository.as_str()),
        ("workspace_path", task.workspace_path.as_str()),
        ("prepared_launch_json", task.prepared_launch_json.as_str()),
        (
            "prepared_launch_sha256",
            task.prepared_launch_sha256.as_str(),
        ),
        ("initial_prompt", task.initial_prompt.as_str()),
    ] {
        if value.trim().is_empty() || value.contains('\0') {
            return Err(StoreError::InvalidState(format!("{name} is invalid")));
        }
    }
    Ok(())
}

fn validate_scope(scope: &TaskQueryScope<'_>) -> StoreResult<()> {
    if scope.repository.is_none() {
        Err(StoreError::InvalidState(
            "repository or group task scope is required".into(),
        ))
    } else {
        Ok(())
    }
}

fn task_result_bytes(result: &TaskResult) -> StoreResult<Vec<u8>> {
    serde_json::to_vec(result).map_err(|error| StoreError::InvalidState(error.to_string()))
}

fn task_result_digest(bytes: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(bytes))
}

fn validate_result(result: &TaskResult) -> StoreResult<()> {
    if result.final_text.trim().is_empty() {
        return Err(StoreError::InvalidState("invalid task result".into()));
    }
    if result.partial && result.outcome == TaskOutcome::Completed {
        return Err(StoreError::InvalidState(
            "partial result cannot be completed".into(),
        ));
    }
    Ok(())
}

type TaskRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    String,
    String,
    String,
    Option<String>,
    i64,
    i64,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    i64,
    i64,
);

fn query_task(connection: &Connection, agent_id: &str) -> StoreResult<Option<TaskRecord>> {
    let row = connection
        .query_row(
        "SELECT agent_id,repository,phase,outcome,
                    workspace_path,runtime_hash,prepared_launch_json,prepared_launch_sha256,
                    initial_prompt,owner_id,owner_epoch,
                    close_requested,stop_requested,failure_code,failure_message,runtime_agent_id,
                    zcode_session_id,turn_state,pid,process_group_id,process_uid,process_start_token,
                    closed_at,reaped_at,created_at,last_event_seq
             FROM tasks WHERE agent_id=?1",
            [agent_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?,
                    row.get(7)?, row.get(8)?, row.get(9)?, row.get(10)?, row.get(11)?,
                    row.get(12)?, row.get(13)?, row.get(14)?, row.get(15)?, row.get(16)?,
                    row.get(17)?, row.get(18)?, row.get(19)?, row.get(20)?, row.get(21)?,
                    row.get(22)?, row.get(23)?, row.get(24)?, row.get(25)?,
                ))
            },
        )
        .optional()?;
    row.map(convert_task_row).transpose()
}

fn convert_task_row(row: TaskRow) -> StoreResult<TaskRecord> {
    let process_identity = match (row.18, row.19, row.20, row.21) {
        (Some(pid), Some(process_group_id), Some(uid), Some(start_token)) => {
            Some(StoredProcessIdentity {
                pid: u32::try_from(pid)
                    .map_err(|_| StoreError::InvalidState("stored pid is invalid".into()))?,
                process_group_id: i32::try_from(process_group_id).map_err(|_| {
                    StoreError::InvalidState("stored process group is invalid".into())
                })?,
                uid: u32::try_from(uid)
                    .map_err(|_| StoreError::InvalidState("stored uid is invalid".into()))?,
                start_token,
            })
        }
        (None, None, None, None) => None,
        _ => {
            return Err(StoreError::InvalidState(
                "stored process identity is incomplete".into(),
            ))
        }
    };
    Ok(TaskRecord {
        agent_id: row.0,
        repository: row.1,
        phase: TaskPhase::parse(&row.2)?,
        outcome: row.3.map(|value| TaskOutcome::parse(&value)).transpose()?,
        workspace_path: row.4,
        runtime_hash: row.5,
        prepared_launch_json: row.6,
        prepared_launch_sha256: row.7,
        initial_prompt: row.8,
        owner_id: row.9,
        owner_epoch: i64_to_u64(row.10)?,
        close_requested: row.11 != 0,
        stop_requested: row.12 != 0,
        failure_code: row.13,
        failure_message: row.14,
        runtime_agent_id: row.15,
        zcode_session_id: row.16,
        turn_state: TurnState::parse(&row.17)?,
        process_identity,
        closed_at: row.22,
        reaped_at: row.23,
        created_at: row.24,
        last_event_seq: i64_to_u64(row.25)?,
    })
}

fn query_guard(
    transaction: &Transaction<'_>,
    agent_id: &str,
) -> StoreResult<(TaskPhase, Option<TaskOutcome>, u64, bool, bool)> {
    let value = transaction
        .query_row(
            "SELECT phase,outcome,owner_epoch,close_requested,stop_requested
             FROM tasks WHERE agent_id=?1",
            [agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| StoreError::InvalidState(format!("unknown task {agent_id}")))?;
    Ok((
        TaskPhase::parse(&value.0)?,
        value
            .1
            .map(|value| TaskOutcome::parse(&value))
            .transpose()?,
        i64_to_u64(value.2)?,
        value.3 != 0,
        value.4 != 0,
    ))
}

fn apply_terminal(
    transaction: &Transaction<'_>,
    agent_id: &str,
    owner_epoch: u64,
    from_phase: TaskPhase,
    close_requested: bool,
    stop_requested: bool,
    terminal: &TerminalUpdate,
) -> StoreResult<TaskOutcome> {
    if from_phase == TaskPhase::Terminal {
        return Err(StoreError::Conflict("task is already terminal".into()));
    }
    let outcome =
        if (stop_requested || close_requested) && terminal.outcome != TaskOutcome::Cancelled {
            TaskOutcome::Cancelled
        } else {
            terminal.outcome
        };
    let now = now_millis();
    let changed = transaction.execute(
        "UPDATE tasks SET phase='TERMINAL',outcome=?1,completed_at=COALESCE(completed_at,?2),
             failure_code=?3,failure_message=?4,
             turn_state=CASE WHEN ?1 IN ('FAILED','RUNTIME_LOST','RESULT_INVALID')
                             THEN 'FAILED' ELSE 'IDLE' END,
             closed_at=CASE WHEN close_requested=1 THEN COALESCE(closed_at,?2) ELSE closed_at END
         WHERE agent_id=?5 AND owner_epoch=?6 AND phase!='TERMINAL'",
        params![
            outcome.as_str(),
            now,
            terminal.failure_code,
            terminal.failure_message,
            agent_id,
            u64_to_i64(owner_epoch)?,
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "terminal transition lost for {agent_id} epoch {owner_epoch}"
        )));
    }
    settle_terminal_commands(
        transaction,
        agent_id,
        terminal
            .failure_code
            .as_deref()
            .unwrap_or("TASK_TERMINATED"),
    )?;
    insert_ledger(
        transaction,
        agent_id,
        owner_epoch,
        Some(from_phase),
        TaskPhase::Terminal,
        Some(outcome),
        terminal.failure_code.as_deref(),
    )?;
    Ok(outcome)
}

fn settle_terminal_commands(
    transaction: &Transaction<'_>,
    agent_id: &str,
    reason_code: &str,
) -> StoreResult<()> {
    transaction.execute(
        "UPDATE messages SET state='FAILED',failure_code=?1,
             failure_message='runtime is no longer available'
         WHERE agent_id=?2 AND state IN ('QUEUED','SENDING')",
        params![reason_code, agent_id],
    )?;
    transaction.execute(
        "DELETE FROM pending_requests
         WHERE agent_id=?1 AND state IN ('PENDING','SENDING')",
        [agent_id],
    )?;
    Ok(())
}

fn insert_ledger(
    transaction: &Transaction<'_>,
    agent_id: &str,
    owner_epoch: u64,
    from_phase: Option<TaskPhase>,
    to_phase: TaskPhase,
    outcome: Option<TaskOutcome>,
    reason_code: Option<&str>,
) -> StoreResult<()> {
    transaction.execute(
        "INSERT INTO lifecycle_ledger(agent_id,owner_epoch,from_phase,to_phase,outcome,
             reason_code,recorded_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            agent_id,
            u64_to_i64(owner_epoch)?,
            from_phase.map(TaskPhase::as_str),
            to_phase.as_str(),
            outcome.map(TaskOutcome::as_str),
            reason_code,
            now_millis(),
        ],
    )?;
    Ok(())
}

fn completion_blockers_tx(connection: &Connection, agent_id: &str) -> StoreResult<(bool, bool)> {
    let pending: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pending_requests WHERE agent_id=?1 AND state IN ('PENDING','SENDING'))",
        [agent_id],
        |row| row.get(0),
    )?;
    let queued: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE agent_id=?1 AND state IN ('QUEUED','SENDING'))",
        [agent_id],
        |row| row.get(0),
    )?;
    Ok((pending, queued))
}

fn query_task_result(
    connection: &Connection,
    agent_id: &str,
) -> StoreResult<Option<StoredTaskResult>> {
    let row = connection
        .query_row(
            "SELECT outcome,final_text,partial,result_sha256
             FROM task_results WHERE agent_id=?1",
            [agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| {
        Ok(StoredTaskResult {
            result: TaskResult {
                outcome: TaskOutcome::parse(&row.0)?,
                final_text: row.1,
                partial: row.2 != 0,
            },
            result_sha256: row.3,
        })
    })
    .transpose()
}

fn query_message(connection: &Connection, message_id: &str) -> StoreResult<Option<StoredMessage>> {
    let row = connection
        .query_row(
            "SELECT message_id,agent_id,mode,content,state,target_turn_id,failure_code
             FROM messages WHERE message_id=?1",
            [message_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| {
        let state = match row.4.as_str() {
            "QUEUED" => MessageState::Queued,
            "SENDING" => MessageState::Sending,
            "DELIVERED" => MessageState::Delivered,
            "FAILED" => MessageState::Failed,
            other => {
                return Err(StoreError::InvalidState(format!(
                    "unknown message state {other}"
                )))
            }
        };
        Ok(StoredMessage {
            message_id: row.0,
            agent_id: row.1,
            mode: row.2,
            content: row.3,
            state,
            target_turn_id: row.5,
            failure_code: row.6,
        })
    })
    .transpose()
}

fn query_pending_request(
    connection: &Connection,
    request_id: &str,
) -> StoreResult<Option<StoredPendingRequest>> {
    let row = connection
        .query_row(
            "SELECT request_id,agent_id,correlation_id,request_type,payload_json,state,
                    response_decision,response_content,created_at
             FROM pending_requests WHERE request_id=?1",
            [request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| {
        let state = match row.5.as_str() {
            "PENDING" => PendingRequestState::Pending,
            "SENDING" => PendingRequestState::Sending,
            "RESPONDED" => PendingRequestState::Responded,
            other => {
                return Err(StoreError::InvalidState(format!(
                    "unknown pending request state {other}"
                )))
            }
        };
        Ok(StoredPendingRequest {
            request_id: row.0,
            agent_id: row.1,
            correlation_id: row.2,
            request_type: row.3,
            payload_json: row.4,
            state,
            response_decision: row.6,
            response_content: row.7,
            created_at: row.8,
        })
    })
    .transpose()
}

fn usize_to_i64(value: usize) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| StoreError::InvalidState("value exceeds SQLite i64".into()))
}

fn u64_to_i64(value: u64) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| StoreError::InvalidState("value exceeds SQLite i64".into()))
}

fn i64_to_u64(value: i64) -> StoreResult<u64> {
    u64::try_from(value).map_err(|_| StoreError::InvalidState("negative SQLite value".into()))
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn task(id: &str, repository: &str, _scope: Option<&str>) -> NewTask {
        NewTask {
            agent_id: id.into(),
            repository: repository.into(),
            workspace_path: format!("/workspace/{id}"),
            runtime_hash: Some("runtime".into()),
            prepared_launch_json: "{}".into(),
            prepared_launch_sha256: "prepared".into(),
            initial_prompt: "do work".into(),
        }
    }

    fn store() -> (TempDir, PathBuf, Store) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        let store = Store::open(&path).unwrap();
        (directory, path, store)
    }

    fn running(store: &Store, id: &str) -> u64 {
        let claim = store.claim_next("daemon", 10, 10).unwrap().unwrap();
        assert_eq!(claim.task.agent_id, id);
        assert!(store
            .mark_session_running(id, claim.owner_epoch, "runtime", None, None, None)
            .unwrap());
        claim.owner_epoch
    }

    fn result(outcome: TaskOutcome) -> TaskResult {
        TaskResult {
            outcome,
            final_text: "terminal text".into(),
            partial: outcome != TaskOutcome::Completed,
        }
    }

    #[test]
    fn fresh_schema_is_minimal_and_forbidden_names_are_absent() {
        let (_directory, _path, store) = store();
        let connection = store.connection.lock().unwrap();
        assert!(schema_is_current(&connection).unwrap());
        assert_eq!(
            connection
                .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let sql: String = connection
            .query_row(
                "SELECT group_concat(sql,' ') FROM sqlite_master WHERE sql IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        for forbidden in [
            concat!("task_", "attempts"),
            concat!("task_", "identities"),
            concat!("compatibility_", "runs"),
            concat!("task_", "kind"),
            concat!("attempt_", "sequence"),
            concat!("checkpoint_", "number"),
            concat!("public_", "agent_id"),
            concat!("execution_", "agent_id"),
        ] {
            assert!(!sql.contains(forbidden), "schema retained {forbidden}");
        }
    }

    #[test]
    fn legacy_schema_is_rejected_without_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy.sqlite3");
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA user_version=8; CREATE TABLE agents(agent_id TEXT PRIMARY KEY);",
                )
                .unwrap();
        }
        let before = fs::read(&path).unwrap();
        let error = match Store::open(&path) {
            Ok(_) => panic!("legacy schema unexpectedly opened"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "STORE_SCHEMA_VERSION_UNSUPPORTED");
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(!path.with_extension("sqlite3-wal").exists());
        assert!(!path.with_extension("sqlite3-shm").exists());
    }

    #[test]
    fn lifecycle_has_one_phase_and_terminal_outcome() {
        let (_directory, _path, store) = store();
        store
            .enqueue_task_authoritative(&task("agent", "/repo", None))
            .unwrap();
        running(&store, "agent");
        store
            .store_task_result("agent", &result(TaskOutcome::Completed))
            .unwrap();
        let task = store.get_task("agent").unwrap().unwrap();
        assert_eq!(task.phase, TaskPhase::Terminal);
        assert_eq!(task.outcome, Some(TaskOutcome::Completed));
        assert_eq!(store.reap_task("agent").unwrap(), TaskOutcome::Completed);
        assert!(store
            .get_task("agent")
            .unwrap()
            .unwrap()
            .reaped_at
            .is_some());
    }

    #[test]
    fn pending_and_cancel_fence_terminal_completion() {
        let (_directory, _path, store) = store();
        store
            .enqueue_task_authoritative(&task("agent", "/repo", None))
            .unwrap();
        running(&store, "agent");
        store
            .insert_pending_request("request", "agent", "1", "permission", "{}")
            .unwrap();
        assert_eq!(
            store.get_task("agent").unwrap().unwrap().phase,
            TaskPhase::WaitingInput
        );
        assert!(matches!(
            store.store_task_result("agent", &result(TaskOutcome::Completed)),
            Err(StoreError::Conflict(_))
        ));
        assert_eq!(
            store
                .claim_pending_response_if_accepting("agent", "request", "deny", None)
                .unwrap(),
            PendingResponseClaimDisposition::Claimed
        );
        store.complete_pending_response("agent", "request").unwrap();
        assert_eq!(
            store.get_task("agent").unwrap().unwrap().phase,
            TaskPhase::Running
        );
        let decision = store.request_stop("agent").unwrap();
        assert_eq!(decision.phase, TaskPhase::Cancelling);
        assert!(store.pending_requests("agent").unwrap().is_empty());
        for outcome in [
            TaskOutcome::Completed,
            TaskOutcome::Failed,
            TaskOutcome::TimedOut,
            TaskOutcome::RuntimeLost,
            TaskOutcome::ResultInvalid,
        ] {
            assert!(matches!(
                store.store_task_result("agent", &result(outcome)),
                Err(StoreError::Conflict(_))
            ));
        }
        store
            .store_task_result("agent", &result(TaskOutcome::Cancelled))
            .unwrap();
    }

    #[test]
    fn close_intent_coerces_non_cancel_terminal_transition() {
        let (_directory, _path, store) = store();
        store
            .enqueue_task_authoritative(&task("agent", "/repo", None))
            .unwrap();
        let owner_epoch = running(&store, "agent");
        store.request_close("agent").unwrap();
        assert_eq!(
            store
                .transition_terminal(
                    "agent",
                    owner_epoch,
                    &TerminalUpdate {
                        outcome: TaskOutcome::RuntimeLost,
                        failure_code: Some("LATE_RUNTIME_LOST".into()),
                        failure_message: None,
                    },
                )
                .unwrap(),
            TaskOutcome::Cancelled
        );
        let task = store.get_task("agent").unwrap().unwrap();
        assert_eq!(task.outcome, Some(TaskOutcome::Cancelled));
        assert!(task.closed_at.is_some());
    }

    #[test]
    fn startup_recovery_inventory_is_read_only() {
        let (_directory, path, store) = store();
        store
            .enqueue_task_authoritative(&task("agent", "/repo", None))
            .unwrap();
        running(&store, "agent");
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened
                .startup_recovery_tasks()
                .unwrap()
                .into_iter()
                .map(|task| task.agent_id)
                .collect::<Vec<_>>(),
            vec!["agent"]
        );
        assert_eq!(
            reopened.get_task("agent").unwrap().unwrap().phase,
            TaskPhase::Running
        );
        assert!(reopened.task_result("agent").unwrap().is_none());
    }
}
