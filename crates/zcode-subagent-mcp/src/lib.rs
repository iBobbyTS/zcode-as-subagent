use rmcp::{
    handler::server::tool::IntoCallToolResult,
    model::{CallToolResponse, CallToolResult, ContentBlock},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use zcode_agentd::rpc::{PendingRequestStateView, PendingRequestView, RpcError, RpcErrorCode};

pub mod server;
pub use server::{serve_stdio, SubagentMcp, PUBLIC_TOOLS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicPendingKind {
    Permission,
    UnsupportedInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicPendingState {
    Pending,
    Sending,
    Responded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicPolicyPreview {
    ExternallyDecidable,
    HardDeny,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicOperation {
    Read,
    Write,
    Command,
    Network,
    GitRefMutation,
    UserInput,
    Unknown,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicPendingRequest {
    pub request_id: String,
    pub kind: PublicPendingKind,
    pub state: PublicPendingState,
    pub respondable: bool,
    pub tool_name: Option<String>,
    pub operation: PublicOperation,
    pub summary: String,
    pub policy_preview: PublicPolicyPreview,
}

impl From<PendingRequestView> for PublicPendingRequest {
    fn from(value: PendingRequestView) -> Self {
        Self {
            request_id: value.request_id,
            kind: if value.kind == "permission" {
                PublicPendingKind::Permission
            } else {
                PublicPendingKind::UnsupportedInput
            },
            state: match value.state {
                PendingRequestStateView::Pending => PublicPendingState::Pending,
                PendingRequestStateView::Sending => PublicPendingState::Sending,
                PendingRequestStateView::Responded => PublicPendingState::Responded,
            },
            respondable: value.respondable,
            tool_name: value.tool_name,
            operation: match value.operation.as_str() {
                "read" => PublicOperation::Read,
                "write" => PublicOperation::Write,
                "command" => PublicOperation::Command,
                "network" => PublicOperation::Network,
                "git_ref_mutation" => PublicOperation::GitRefMutation,
                "user_input" => PublicOperation::UserInput,
                _ => PublicOperation::Unknown,
            },
            summary: value.summary,
            policy_preview: match value.policy_preview.as_str() {
                "externally_decidable" => PublicPolicyPreview::ExternallyDecidable,
                "hard_deny" => PublicPolicyPreview::HardDeny,
                _ => PublicPolicyPreview::Unknown,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicResponseDisposition {
    Responded,
    AlreadyResponded,
    InFlight,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicErrorEnvelope {
    pub error: PublicToolErrorBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PublicToolErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    pub body: PublicToolErrorBody,
    pub legacy_text: String,
}

impl ToolError {
    pub(crate) fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        legacy_text: impl Into<String>,
        component: &'static str,
    ) -> Self {
        Self {
            body: PublicToolErrorBody {
                code: code.into(),
                message: message.into(),
                component: Some(component.into()),
                operation: None,
                request_id: None,
                agent_id: None,
                cleanup: None,
            },
            legacy_text: legacy_text.into(),
        }
    }

    pub(crate) fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.body.operation = Some(operation.into());
        self
    }

    pub(crate) fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.body.request_id = Some(request_id.into());
        self
    }

    pub(crate) fn with_agent_id(mut self, agent_id: Option<String>) -> Self {
        if self.body.agent_id.is_none() {
            self.body.agent_id = agent_id;
        }
        self
    }
}

impl IntoCallToolResult for ToolError {
    fn into_call_tool_result(self) -> Result<CallToolResponse, rmcp::ErrorData> {
        let structured = serde_json::to_value(PublicErrorEnvelope { error: self.body })
            .map_err(|error| rmcp::ErrorData::internal_error(error.to_string(), None))?;
        let mut result = CallToolResult::structured_error(structured);
        // structured_error defaults content to serialized JSON. Preserve the
        // bounded legacy text for existing human and string consumers.
        result.content = vec![ContentBlock::text(self.legacy_text)];
        Ok(result.into())
    }
}

pub(crate) fn validation_error(detail: impl Into<String>) -> ToolError {
    let detail = detail.into();
    ToolError::new(
        "validation",
        "request validation failed",
        format!("validation: {detail}"),
        "facade",
    )
}

pub(crate) fn public_error(error: RpcError) -> ToolError {
    let detail = error.message.clone();
    let (code, message) = match error.code {
        RpcErrorCode::Malformed | RpcErrorCode::Validation => {
            ("validation", "request validation failed")
        }
        RpcErrorCode::Oversized => ("oversized", "bounded response or request was too large"),
        RpcErrorCode::UnsupportedVersion => {
            ("protocol_version_mismatch", "incompatible subagent daemon")
        }
        RpcErrorCode::UnknownMethod => ("protocol_error", "daemon method is unavailable"),
        RpcErrorCode::NotFound => ("not_found", "agent task was not found"),
        RpcErrorCode::Conflict => (
            "conflict",
            match detail.as_str() {
                "WORKSPACE_BUSY" => "WORKSPACE_BUSY",
                "MESSAGE_ID_CONFLICT" => "MESSAGE_ID_CONFLICT",
                _ => "durable state conflict",
            },
        ),
        RpcErrorCode::Unavailable if detail == "RUNTIME_COMMAND_FAILED" => (
            "runtime_command_failed",
            "runtime could not complete the command",
        ),
        RpcErrorCode::Timeout => ("timeout", "daemon operation timed out"),
        RpcErrorCode::RuntimeLost => ("runtime_lost", "agent runtime was lost"),
        RpcErrorCode::ResultInvalid => ("result_invalid", "stored task result failed verification"),
        RpcErrorCode::Persistence => ("persistence", "durable store operation failed"),
        RpcErrorCode::Internal => ("internal", "daemon operation failed"),
        RpcErrorCode::Unavailable => (
            "unavailable",
            "subagent daemon could not complete the operation",
        ),
    };
    let agent_id = error.active_agent_id;
    let legacy_text = if let Some(agent_id) = agent_id.as_ref() {
        format!("{code}: {message} (active_agent_id={agent_id})")
    } else if matches!(
        error.code,
        RpcErrorCode::Validation | RpcErrorCode::Malformed
    ) && detail != message
        && detail.len() <= 512
    {
        format!("{code}: {message}: {detail}")
    } else {
        format!("{code}: {message}")
    };
    ToolError::new(code, message, legacy_text, "daemon").with_agent_id(agent_id)
}

pub(crate) fn public_transport_error(error: std::io::Error) -> ToolError {
    let (code, message, legacy_text) = match error.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => (
            "timeout",
            "daemon call exceeded its bound",
            "timeout: daemon call exceeded its bound",
        ),
        std::io::ErrorKind::InvalidData => (
            "protocol_error",
            "daemon returned an invalid or oversized frame",
            "protocol_error: daemon returned an invalid or oversized frame",
        ),
        _ => (
            "daemon_unavailable",
            "subagent daemon is unavailable",
            "daemon_unavailable: subagent daemon is unavailable",
        ),
    };
    ToolError::new(code, message, legacy_text, "daemon_transport")
}

pub(crate) fn protocol_error() -> ToolError {
    ToolError::new(
        "protocol_error",
        "unexpected daemon response",
        "protocol_error: unexpected daemon response",
        "facade",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_distinguish_conflicts_rejections_and_unreachable_socket() {
        assert_eq!(
            public_error(RpcError::new(RpcErrorCode::Conflict, "MESSAGE_ID_CONFLICT")).legacy_text,
            "conflict: MESSAGE_ID_CONFLICT"
        );
        assert_eq!(
            public_error(RpcError::new(
                RpcErrorCode::Conflict,
                "private store details"
            ))
            .legacy_text,
            "conflict: durable state conflict"
        );
        assert!(public_error(RpcError::new(
            RpcErrorCode::Unavailable,
            "RUNTIME_COMMAND_FAILED"
        ))
        .legacy_text
        .starts_with("runtime_command_failed:"));
        assert!(public_transport_error(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused
        ))
        .legacy_text
        .starts_with("daemon_unavailable:"));
    }

    #[test]
    fn workspace_busy_preserves_code_message_and_active_agent() {
        let mut error = RpcError::new(RpcErrorCode::Conflict, "WORKSPACE_BUSY");
        error.active_agent_id = Some("agent-42".into());
        let rendered = public_error(error);
        assert!(rendered.legacy_text.starts_with("conflict: WORKSPACE_BUSY"));
        assert!(rendered.legacy_text.contains("active_agent_id=agent-42"));
        assert_eq!(rendered.body.code, "conflict");
        assert_eq!(rendered.body.agent_id.as_deref(), Some("agent-42"));
    }

    #[test]
    fn structured_error_keeps_legacy_text_and_machine_fields_in_sync() {
        let result = public_transport_error(std::io::Error::from(std::io::ErrorKind::TimedOut))
            .with_operation("poll")
            .with_request_id("request-7")
            .with_agent_id(Some("agent-7".into()))
            .into_call_tool_result()
            .unwrap();
        let rmcp::model::CallToolResponse::Complete(result) = result else {
            panic!("expected complete tool result")
        };
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.content[0].as_text().unwrap().text,
            "timeout: daemon call exceeded its bound"
        );
        let error = &result.structured_content.unwrap()["error"];
        assert_eq!(error["code"], "timeout");
        assert_eq!(error["component"], "daemon_transport");
        assert_eq!(error["operation"], "poll");
        assert_eq!(error["request_id"], "request-7");
        assert_eq!(error["agent_id"], "agent-7");
    }

    #[test]
    fn typed_sentinels_keep_distinct_machine_codes_without_message_parsing() {
        let cases = [
            (
                RpcErrorCode::Unavailable,
                "RUNTIME_COMMAND_FAILED",
                "runtime_command_failed",
            ),
            (RpcErrorCode::Timeout, "private timeout detail", "timeout"),
            (
                RpcErrorCode::RuntimeLost,
                "private driver detail",
                "runtime_lost",
            ),
            (
                RpcErrorCode::ResultInvalid,
                "private digest detail",
                "result_invalid",
            ),
            (RpcErrorCode::NotFound, "private lookup detail", "not_found"),
        ];
        for (kind, detail, expected) in cases {
            let projected = public_error(RpcError::new(kind, detail));
            assert_eq!(projected.body.code, expected);
            assert!(projected.legacy_text.starts_with(expected));
        }
        let validation = public_error(RpcError::new(
            RpcErrorCode::Validation,
            "agent_id is invalid",
        ));
        assert_eq!(validation.body.code, "validation");
        assert!(validation.legacy_text.contains("agent_id is invalid"));
    }
}
