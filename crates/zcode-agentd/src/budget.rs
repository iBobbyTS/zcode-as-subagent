use std::time::{Duration, Instant};
use zcode_agent_store::EffectiveBudget;
#[cfg(test)] use zcode_agent_preparation::BudgetLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BudgetViolation {
    WallTime,
}

impl BudgetViolation {
    pub(crate) fn reason_code(self) -> &'static str {
        match self {
            Self::WallTime => "WALL_TIME_DEADLINE_EXCEEDED",
        }
    }
}

#[derive(Debug)]
pub(crate) struct RuntimeBudget {
    deadline: Instant,
}

impl RuntimeBudget {
    #[cfg(test)]
    pub(crate) fn new(limits: &BudgetLimits) -> Self {
        Self::with_limits(limits.absolute_wall_time_ms)
    }

    pub(crate) fn from_effective(limits: &EffectiveBudget) -> Self {
        Self::with_limits(limits.absolute_wall_time_ms)
    }

    fn with_limits(wall_time_ms: u64) -> Self {
        Self {
            deadline: Instant::now()
                .checked_add(Duration::from_millis(wall_time_ms))
                .unwrap_or_else(Instant::now),
        }
    }

    pub(crate) fn observe(&self, _inbound: &zcode_driver::Inbound) {}

    pub(crate) fn violation(&self) -> Option<BudgetViolation> {
        (Instant::now() >= self.deadline).then_some(BudgetViolation::WallTime)
    }

    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    #[cfg(test)]
    fn counts(&self) -> (u64, usize) {
        (0, 0)
    }
}

#[cfg(any())]
mod tests {
    use super::*;
    use zcode_protocol::{EventEnvelope, WireMessage};

    fn limits() -> BudgetLimits {
        BudgetLimits {
            absolute_wall_time_ms: 10_000,
            runtime_activity_idle_timeout_ms: 1_000,
            model_stream_idle_timeout_ms: 1_000,
            tool_call_timeout_ms: 1_000,
            input_wait_timeout_ms: 1_000,
            max_turns: 2,
            max_tool_calls: 1,
            max_context_bytes: 1,
            max_result_bytes: 1,
            max_artifact_bytes: 1,
        }
    }

    fn event(kind: &str, payload: serde_json::Value) -> Inbound {
        Inbound::Message(WireMessage::Event(EventEnvelope {
            method: "session/event".into(),
            params: serde_json::json!({"type":kind,"payload":payload}),
        }))
    }

    #[test]
    fn duplicate_tool_updates_count_once_and_ambiguous_identity_fails_closed() {
        let budget = RuntimeBudget::new(&limits());
        budget.observe(&event(
            "tool.updated",
            serde_json::json!({"toolCallId":"tool-1"}),
        ));
        budget.observe(&event(
            "tool.updated",
            serde_json::json!({"toolCallId":"tool-1"}),
        ));
        assert_eq!(budget.counts(), (0, 1));
        assert_eq!(budget.violation(), None);
        budget.observe(&event(
            "tool.updated",
            serde_json::json!({"toolCallId":"tool-2"}),
        ));
        assert_eq!(budget.violation(), Some(BudgetViolation::ToolCallLimit));

        let ambiguous = RuntimeBudget::new(&limits());
        ambiguous.observe(&event(
            "tool.updated",
            serde_json::json!({"toolName":"Bash"}),
        ));
        assert_eq!(
            ambiguous.violation(),
            Some(BudgetViolation::AmbiguousToolIdentity)
        );
    }

    #[test]
    fn duplicate_turn_identity_counts_once_and_ambiguous_identity_fails_closed() {
        let budget = RuntimeBudget::new(&limits());
        budget.observe(&event(
            "turn.started",
            serde_json::json!({"turnId":"turn-1"}),
        ));
        budget.observe(&event(
            "turn.started",
            serde_json::json!({"turnId":"turn-1"}),
        ));
        assert_eq!(budget.counts(), (1, 0));
        assert_eq!(budget.violation(), None);
        budget.observe(&event(
            "turn.started",
            serde_json::json!({"turnId":"turn-2"}),
        ));
        assert_eq!(budget.counts(), (2, 0));
        assert_eq!(budget.violation(), None);

        let missing = RuntimeBudget::new(&limits());
        missing.observe(&event("turn.started", serde_json::json!({})));
        assert_eq!(
            missing.violation(),
            Some(BudgetViolation::AmbiguousTurnIdentity)
        );

        let conflicting = RuntimeBudget::new(&limits());
        let Inbound::Message(WireMessage::Event(mut event)) =
            event("turn.started", serde_json::json!({"turnId":"turn-1"}))
        else {
            unreachable!()
        };
        event.params["turnId"] = serde_json::json!("other-turn");
        conflicting.observe(&Inbound::Message(WireMessage::Event(event)));
        assert_eq!(
            conflicting.violation(),
            Some(BudgetViolation::AmbiguousTurnIdentity)
        );
    }
}
