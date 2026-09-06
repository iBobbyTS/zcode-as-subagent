use std::time::{Duration, Instant};
use zcode_agent_preparation::RuntimeTimeouts;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeoutViolation {
    WallTime,
}

impl TimeoutViolation {
    pub(crate) fn reason_code(self) -> &'static str {
        match self {
            Self::WallTime => "WALL_TIME_DEADLINE_EXCEEDED",
        }
    }
}

#[derive(Debug)]
pub(crate) struct RuntimeDeadline {
    deadline: Instant,
}

impl RuntimeDeadline {
    pub(crate) fn from_timeouts(limits: &RuntimeTimeouts) -> Self {
        Self {
            deadline: Instant::now()
                .checked_add(Duration::from_millis(limits.absolute_wall_time_ms))
                .unwrap_or_else(Instant::now),
        }
    }

    pub(crate) fn observe(&self, _inbound: &zcode_driver::Inbound) {}

    pub(crate) fn violation(&self) -> Option<TimeoutViolation> {
        (Instant::now() >= self.deadline).then_some(TimeoutViolation::WallTime)
    }

    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
}
