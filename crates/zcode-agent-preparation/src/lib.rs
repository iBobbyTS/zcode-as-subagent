mod general;
mod policy;

pub use general::PermissionMode;
pub use policy::AGENT_BASH_COMMAND_FAMILIES;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

pub const AGENT_FILE_POLICY_VERSION: &str = "zcode-agent-file-policy/v1.0.0";
const AGENT_BASH_POLICY_VERSION: &str = "zcode-agent-bash/v1.0.0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHookProvenance {
    #[serde(default)]
    pub effective_file_policy_version: Option<String>,
    #[serde(default)]
    pub effective_file_policy_sha256: Option<String>,
    #[serde(default)]
    pub effective_file_policy_path: Option<String>,
    #[serde(default)]
    pub effective_config_path: Option<String>,
    #[serde(default)]
    pub effective_config_sha256: Option<String>,
    #[serde(default)]
    pub effective_guard_wrapper_path: Option<String>,
    #[serde(default)]
    pub effective_guard_wrapper_sha256: Option<String>,
    #[serde(default)]
    pub effective_audit_wrapper_path: Option<String>,
    #[serde(default)]
    pub effective_audit_wrapper_sha256: Option<String>,
    #[serde(default)]
    pub effective_file_wrapper_path: Option<String>,
    #[serde(default)]
    pub effective_file_wrapper_sha256: Option<String>,
    pub hook_activation_verified: bool,
    pub activation_method: Option<String>,
    pub activation_generation: Option<String>,
    #[serde(default)]
    pub service_generation: Option<String>,
}

impl Default for AgentHookProvenance {
    fn default() -> Self {
        agent_hook_provenance()
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Load and verify the installed hook record. The service identity is generated
/// and persisted by installation; callers do not provide an environment value.
pub fn agent_hook_provenance() -> AgentHookProvenance {
    let unverified = || AgentHookProvenance {
        effective_file_policy_version: None,
        effective_file_policy_sha256: None,
        effective_file_policy_path: None,
        effective_config_path: None,
        effective_config_sha256: None,
        effective_guard_wrapper_path: None,
        effective_guard_wrapper_sha256: None,
        effective_audit_wrapper_path: None,
        effective_audit_wrapper_sha256: None,
        effective_file_wrapper_path: None,
        effective_file_wrapper_sha256: None,
        hook_activation_verified: false,
        activation_method: None,
        activation_generation: None,
        service_generation: None,
    };
    let Some(path) = std::env::var_os("ZCODE_AGENT_HOOK_PROVENANCE") else {
        return unverified();
    };
    let Ok(bytes) = fs::read(path) else {
        return unverified();
    };
    let Ok(record) = serde_json::from_slice::<AgentHookProvenance>(&bytes) else {
        return unverified();
    };
    let file_policy_matches = file_hash_matches(
        record.effective_file_policy_path.as_deref(),
        record.effective_file_policy_sha256.as_deref(),
    );
    let verified = record.hook_activation_verified
        && record.effective_file_policy_version.as_deref() == Some(AGENT_FILE_POLICY_VERSION)
        && file_policy_matches
        && effective_config_references_hook(&record)
        && record
            .activation_method
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        && record
            .activation_generation
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        && record
            .service_generation
            .as_deref()
            .is_some_and(|value| !value.is_empty());
    if verified {
        record
    } else {
        unverified()
    }
}

fn file_hash_matches(path: Option<&str>, expected: Option<&str>) -> bool {
    match (path, expected) {
        (Some(path), Some(expected)) => fs::read(path)
            .ok()
            .is_some_and(|bytes| sha256_bytes(&bytes) == expected),
        _ => false,
    }
}

fn effective_config_references_hook(record: &AgentHookProvenance) -> bool {
    let (
        Some(config_path),
        Some(config_sha256),
        Some(guard_path),
        Some(guard_sha256),
        Some(audit_path),
        Some(audit_sha256),
        Some(file_policy_path),
        Some(file_wrapper_path),
        Some(file_wrapper_sha256),
    ) = (
        record.effective_config_path.as_deref(),
        record.effective_config_sha256.as_deref(),
        record.effective_guard_wrapper_path.as_deref(),
        record.effective_guard_wrapper_sha256.as_deref(),
        record.effective_audit_wrapper_path.as_deref(),
        record.effective_audit_wrapper_sha256.as_deref(),
        record.effective_file_policy_path.as_deref(),
        record.effective_file_wrapper_path.as_deref(),
        record.effective_file_wrapper_sha256.as_deref(),
    )
    else {
        return false;
    };
    if !file_hash_matches(Some(config_path), Some(config_sha256))
        || !file_hash_matches(Some(guard_path), Some(guard_sha256))
        || !file_hash_matches(Some(audit_path), Some(audit_sha256))
        || !file_hash_matches(
            Some(file_policy_path),
            record.effective_file_policy_sha256.as_deref(),
        )
        || !file_hash_matches(Some(file_wrapper_path), Some(file_wrapper_sha256))
    {
        return false;
    }
    let Some(hook_root) = PathBuf::from(guard_path)
        .parent()
        .and_then(|path| path.parent())
        .map(PathBuf::from)
    else {
        return false;
    };
    if Path::new(audit_path) != hook_root.join("hooks/audit-bash-result.mjs")
        || Path::new(file_policy_path) != hook_root.join("lib/agent-file-policy.mjs")
        || Path::new(file_wrapper_path) != hook_root.join("hooks/check-agent-files.mjs")
    {
        return false;
    }
    let Ok(config) = fs::read(config_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .ok_or(())
    else {
        return false;
    };
    if config
        .pointer("/hooks/enabled")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return false;
    }
    [
        ("PreToolUse", guard_path),
        ("PostToolUse", audit_path),
        ("PostToolUseFailure", audit_path),
    ]
    .into_iter()
    .all(|(event, expected_path)| config_event_references(&config, event, "Bash", expected_path))
        && config_event_references(
            &config,
            "PreToolUse",
            "^(Read|Grep|Glob|Write|Edit|Delete|Move)$",
            file_wrapper_path,
        )
}

fn config_event_references(
    config: &serde_json::Value,
    event: &str,
    matcher: &str,
    expected_path: &str,
) -> bool {
    config
        .pointer(&format!("/hooks/events/{event}"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|entries| {
            let matching = entries
                .iter()
                .filter(|entry| {
                    entry.get("matcher").and_then(serde_json::Value::as_str) == Some(matcher)
                })
                .collect::<Vec<_>>();
            matching.len() == 1
                && matching.into_iter().all(|entry| {
                    let hook = entry
                        .get("hooks")
                        .and_then(serde_json::Value::as_array)
                        .filter(|hooks| hooks.len() == 1)
                        .and_then(|hooks| hooks.first());
                    entry.get("description").is_none()
                        && hook
                            .and_then(|value| value.get("type"))
                            .and_then(serde_json::Value::as_str)
                            == Some("process")
                        && hook
                            .and_then(|value| value.get("timeoutMs"))
                            .and_then(serde_json::Value::as_u64)
                            == Some(5_000)
                        && hook
                            .and_then(|value| value.get("command"))
                            .and_then(serde_json::Value::as_str)
                            == Some("node")
                        && hook
                            .and_then(|value| value.get("args"))
                            .and_then(serde_json::Value::as_array)
                            .and_then(|args| (args.len() == 1).then(|| args[0].as_str()))
                            .flatten()
                            == Some(expected_path)
                })
        })
}

#[cfg(test)]
mod provenance_tests {
    use super::agent_hook_provenance;

    #[test]
    fn missing_record_cannot_verify() {
        let provenance = agent_hook_provenance();
        assert!(!provenance.hook_activation_verified);
        assert!(provenance.service_generation.is_none());
    }
}

pub use general::{
    canonical_general_repository, general_control_header, general_launch_prompt, AccessMode,
    CompletionOutcome, GeneralCompletion, GeneralFinalizer, GeneralTaskManifest,
    GeneralTaskPreparer, PreparedGeneralTask, PreparedWorkspace, GENERAL_CONTROL_SCHEMA,
    GENERAL_TASK_SCHEMA,
};

pub use policy::{
    ExternalDecision, PermissionDecision, PermissionRequest, PolicyCapabilities, PolicyLauncher,
    SandboxEnforcement, ValidatedPermissionDenial,
};

#[derive(Debug)]
pub enum PreparationError {
    InvalidManifest(String),
    InvalidPath { path: PathBuf, reason: String },
    PathEscape { path: PathBuf, root: PathBuf },
    SymlinkInput(PathBuf),
    MissingInput(PathBuf),
    ForbiddenInput(PathBuf),
    CredentialInput(PathBuf),
    Policy(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl PreparationError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidManifest(_) => "INVALID_MANIFEST",
            Self::InvalidPath { .. } => "INVALID_PATH",
            Self::PathEscape { .. } => "PATH_ESCAPE",
            Self::SymlinkInput(_) => "SYMLINK_INPUT",
            Self::MissingInput(_) => "MISSING_INPUT",
            Self::ForbiddenInput(_) => "FORBIDDEN_INPUT",
            Self::CredentialInput(_) => "CREDENTIAL_INPUT",
            Self::Policy(_) => "POLICY_DENIED",
            Self::Io(_) => "IO_ERROR",
            Self::Json(_) => "JSON_ERROR",
        }
    }
}

impl fmt::Display for PreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest(message) => write!(formatter, "invalid manifest: {message}"),
            Self::InvalidPath { path, reason } => {
                write!(formatter, "invalid path {}: {reason}", path.display())
            }
            Self::PathEscape { path, root } => write!(
                formatter,
                "path {} escapes allowed root {}",
                path.display(),
                root.display()
            ),
            Self::SymlinkInput(path) => {
                write!(formatter, "symlink input is forbidden: {}", path.display())
            }
            Self::MissingInput(path) => write!(formatter, "input is missing: {}", path.display()),
            Self::ForbiddenInput(path) => {
                write!(
                    formatter,
                    "forbidden agent metadata input is forbidden: {}",
                    path.display()
                )
            }
            Self::CredentialInput(path) => {
                write!(
                    formatter,
                    "credential input is forbidden: {}",
                    path.display()
                )
            }
            Self::Policy(message) => write!(formatter, "policy denied request: {message}"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Json(error) => write!(formatter, "JSON error: {error}"),
        }
    }
}

impl std::error::Error for PreparationError {}

impl From<std::io::Error> for PreparationError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for PreparationError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub type PreparationResult<T> = Result<T, PreparationError>;
