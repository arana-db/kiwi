// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Feature-gated request lifecycle contract for the storage runtime baseline harness.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use uuid::Uuid;

use crate::RequestId;

/// Logical identity shared by all physical attempts for one client request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct LogicalRequestId(Uuid);

impl LogicalRequestId {
    /// Create a new logical request identity.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Return the underlying UUID.
    pub fn inner(self) -> Uuid {
        self.0
    }
}

impl Default for LogicalRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for LogicalRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Identity carried unchanged across the complete lifetime of one physical attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BaselineTrace {
    pub logical_id: LogicalRequestId,
    pub attempt_id: RequestId,
    pub attempt_index: u32,
}

/// Observable lifecycle states for an accepted storage attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[repr(u8)]
pub enum AttemptState {
    Offered = 0,
    ChannelQueued = 1,
    BatchQueued = 2,
    WaitingGate = 3,
    Running = 4,
    ExecutionFinished = 5,
    ShutdownRejectedAfterAccept = 6,
    Abandoned = 7,
}

impl AttemptState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Offered,
            1 => Self::ChannelQueued,
            2 => Self::BatchQueued,
            3 => Self::WaitingGate,
            4 => Self::Running,
            5 => Self::ExecutionFinished,
            6 => Self::ShutdownRejectedAfterAccept,
            7 => Self::Abandoned,
            _ => unreachable!("baseline attempt state is only written from AttemptState"),
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::ExecutionFinished | Self::ShutdownRejectedAfterAccept | Self::Abandoned
        )
    }

    fn permits(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Offered, Self::ChannelQueued)
                | (Self::ChannelQueued, Self::BatchQueued | Self::WaitingGate)
                | (Self::BatchQueued, Self::WaitingGate)
                | (Self::WaitingGate, Self::Running)
                | (
                    Self::ChannelQueued | Self::BatchQueued | Self::WaitingGate,
                    Self::ShutdownRejectedAfterAccept
                )
                | (Self::Running, Self::ExecutionFinished)
        )
    }
}

/// Storage execution result, independent of response delivery to the client.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ExecutionOutcome {
    Success,
    CommandError,
    InternalFailure,
}

/// Synchronous events emitted by a baseline attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum BaselineEvent {
    StateTransition {
        trace: BaselineTrace,
        previous: AttemptState,
        next: AttemptState,
        elapsed: Duration,
        outcome: Option<ExecutionOutcome>,
    },
    ClientTimeout {
        trace: BaselineTrace,
        elapsed: Duration,
    },
    ResponseDropped {
        trace: BaselineTrace,
        elapsed: Duration,
    },
    ResponseDelivered {
        trace: BaselineTrace,
        elapsed: Duration,
    },
    InvariantViolation {
        trace: BaselineTrace,
        previous: AttemptState,
        attempted: AttemptState,
        elapsed: Duration,
    },
}

/// Consumer of baseline lifecycle events.
pub trait BaselineObserver: Send + Sync + 'static {
    fn on_event(&self, event: BaselineEvent);
    fn before_execute(&self, trace: &BaselineTrace);
}

/// Error returned when a lifecycle transition violates the frozen state graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid baseline attempt transition from {previous:?} to {attempted:?}")]
pub struct BaselineTransitionError {
    previous: AttemptState,
    attempted: AttemptState,
}

impl BaselineTransitionError {
    pub fn previous(self) -> AttemptState {
        self.previous
    }

    pub fn attempted(self) -> AttemptState {
        self.attempted
    }
}

struct BaselineAttemptInner {
    trace: BaselineTrace,
    state: AtomicU8,
    started_at: Instant,
    last_transition: Mutex<Instant>,
    observer: Arc<dyn BaselineObserver>,
}

/// Cloneable RAII token for one physical storage attempt.
#[derive(Clone)]
pub struct BaselineAttempt {
    inner: Arc<BaselineAttemptInner>,
}

impl BaselineAttempt {
    pub fn new(
        logical_id: LogicalRequestId,
        attempt_index: u32,
        observer: Arc<dyn BaselineObserver>,
    ) -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(BaselineAttemptInner {
                trace: BaselineTrace {
                    logical_id,
                    attempt_id: RequestId::new(),
                    attempt_index,
                },
                state: AtomicU8::new(AttemptState::Offered as u8),
                started_at: now,
                last_transition: Mutex::new(now),
                observer,
            }),
        }
    }

    pub fn trace(&self) -> BaselineTrace {
        self.inner.trace
    }

    pub fn state(&self) -> AttemptState {
        AttemptState::from_u8(self.inner.state.load(Ordering::Acquire))
    }

    pub fn transition(&self, next: AttemptState) -> Result<(), BaselineTransitionError> {
        self.transition_inner(next, None)
    }

    pub fn finish_execution(
        &self,
        outcome: ExecutionOutcome,
    ) -> Result<(), BaselineTransitionError> {
        self.transition_inner(AttemptState::ExecutionFinished, Some(outcome))
    }

    pub fn before_execute(&self) {
        self.inner.observer.before_execute(&self.inner.trace);
    }

    pub fn record_client_timeout(&self) {
        self.inner.observer.on_event(BaselineEvent::ClientTimeout {
            trace: self.inner.trace,
            elapsed: self.inner.started_at.elapsed(),
        });
    }

    pub fn record_response_dropped(&self) {
        self.inner
            .observer
            .on_event(BaselineEvent::ResponseDropped {
                trace: self.inner.trace,
                elapsed: self.inner.started_at.elapsed(),
            });
    }

    pub fn record_response_delivered(&self) {
        self.inner
            .observer
            .on_event(BaselineEvent::ResponseDelivered {
                trace: self.inner.trace,
                elapsed: self.inner.started_at.elapsed(),
            });
    }

    fn transition_inner(
        &self,
        next: AttemptState,
        outcome: Option<ExecutionOutcome>,
    ) -> Result<(), BaselineTransitionError> {
        let (previous, elapsed) = {
            let mut last_transition = self
                .inner
                .last_transition
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = self.state();
            if !previous.permits(next)
                || (next == AttemptState::ExecutionFinished && outcome.is_none())
                || (next != AttemptState::ExecutionFinished && outcome.is_some())
            {
                drop(last_transition);
                self.record_invariant_violation(previous, next);
                return Err(BaselineTransitionError {
                    previous,
                    attempted: next,
                });
            }

            self.inner.state.store(next as u8, Ordering::Release);
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(*last_transition);
            *last_transition = now;
            (previous, elapsed)
        };

        self.inner
            .observer
            .on_event(BaselineEvent::StateTransition {
                trace: self.inner.trace,
                previous,
                next,
                elapsed,
                outcome,
            });
        Ok(())
    }

    fn record_invariant_violation(&self, previous: AttemptState, attempted: AttemptState) {
        self.inner
            .observer
            .on_event(BaselineEvent::InvariantViolation {
                trace: self.inner.trace,
                previous,
                attempted,
                elapsed: self.inner.started_at.elapsed(),
            });
    }
}

impl fmt::Debug for BaselineAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BaselineAttempt")
            .field("trace", &self.trace())
            .field("state", &self.state())
            .finish()
    }
}

impl Drop for BaselineAttempt {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }

        let transition = {
            let mut last_transition = self
                .inner
                .last_transition
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = self.state();
            if previous.is_terminal() {
                None
            } else {
                self.inner
                    .state
                    .store(AttemptState::Abandoned as u8, Ordering::Release);
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(*last_transition);
                *last_transition = now;
                Some((previous, elapsed))
            }
        };

        if let Some((previous, elapsed)) = transition {
            self.inner
                .observer
                .on_event(BaselineEvent::StateTransition {
                    trace: self.inner.trace,
                    previous,
                    next: AttemptState::Abandoned,
                    elapsed,
                    outcome: None,
                });
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<BaselineEvent>>,
    }

    impl RecordingObserver {
        fn events(&self) -> Vec<BaselineEvent> {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl BaselineObserver for RecordingObserver {
        fn on_event(&self, event: BaselineEvent) {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event);
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    fn attempt(observer: Arc<RecordingObserver>) -> BaselineAttempt {
        BaselineAttempt::new(LogicalRequestId::new(), 0, observer)
    }

    #[test]
    fn legal_transitions_record_previous_and_next_state() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());

        attempt.transition(AttemptState::ChannelQueued).unwrap();
        attempt.transition(AttemptState::BatchQueued).unwrap();
        attempt.transition(AttemptState::WaitingGate).unwrap();
        attempt.transition(AttemptState::Running).unwrap();
        attempt.finish_execution(ExecutionOutcome::Success).unwrap();

        assert_eq!(attempt.state(), AttemptState::ExecutionFinished);
        let transitions: Vec<_> = observer
            .events()
            .into_iter()
            .filter_map(|event| match event {
                BaselineEvent::StateTransition {
                    previous,
                    next,
                    elapsed,
                    ..
                } => Some((previous, next, elapsed)),
                _ => None,
            })
            .collect();
        assert_eq!(transitions.len(), 5);
        assert_eq!(
            transitions
                .iter()
                .map(|(previous, next, _)| (*previous, *next))
                .collect::<Vec<_>>(),
            vec![
                (AttemptState::Offered, AttemptState::ChannelQueued),
                (AttemptState::ChannelQueued, AttemptState::BatchQueued),
                (AttemptState::BatchQueued, AttemptState::WaitingGate),
                (AttemptState::WaitingGate, AttemptState::Running),
                (AttemptState::Running, AttemptState::ExecutionFinished),
            ]
        );
    }

    #[test]
    fn illegal_transition_returns_error_and_records_invariant_violation() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());

        let error = attempt.transition(AttemptState::Running).unwrap_err();

        assert_eq!(error.previous(), AttemptState::Offered);
        assert_eq!(error.attempted(), AttemptState::Running);
        assert_eq!(attempt.state(), AttemptState::Offered);
        assert!(matches!(
            observer.events().as_slice(),
            [BaselineEvent::InvariantViolation {
                previous: AttemptState::Offered,
                attempted: AttemptState::Running,
                ..
            }]
        ));
    }

    #[test]
    fn repeated_or_post_terminal_transition_is_rejected() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        attempt.transition(AttemptState::ChannelQueued).unwrap();
        attempt.transition(AttemptState::WaitingGate).unwrap();
        attempt.transition(AttemptState::Running).unwrap();
        attempt
            .finish_execution(ExecutionOutcome::CommandError)
            .unwrap();

        assert!(
            attempt
                .finish_execution(ExecutionOutcome::InternalFailure)
                .is_err()
        );
        assert!(attempt.transition(AttemptState::Abandoned).is_err());
        assert_eq!(attempt.state(), AttemptState::ExecutionFinished);
        assert_eq!(
            observer
                .events()
                .into_iter()
                .filter(|event| matches!(event, BaselineEvent::InvariantViolation { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn last_token_drop_marks_non_terminal_attempt_abandoned_once() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        let trace = attempt.trace();
        attempt.transition(AttemptState::ChannelQueued).unwrap();

        drop(attempt);

        let abandonments: Vec<_> = observer
            .events()
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    BaselineEvent::StateTransition {
                        trace: event_trace,
                        next: AttemptState::Abandoned,
                        ..
                    } if *event_trace == trace
                )
            })
            .collect();
        assert_eq!(abandonments.len(), 1);
    }

    #[test]
    fn dropping_one_clone_does_not_abandon_shared_attempt() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        attempt.transition(AttemptState::ChannelQueued).unwrap();
        let clone = attempt.clone();

        drop(clone);

        assert_eq!(attempt.state(), AttemptState::ChannelQueued);
        assert!(!observer.events().iter().any(|event| matches!(
            event,
            BaselineEvent::StateTransition {
                next: AttemptState::Abandoned,
                ..
            }
        )));
        attempt
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }

    #[test]
    fn retry_attempts_share_logical_identity_but_not_physical_identity() {
        let observer = Arc::new(RecordingObserver::default());
        let logical_id = LogicalRequestId::new();
        let first = BaselineAttempt::new(logical_id, 0, observer.clone());
        let retry = BaselineAttempt::new(logical_id, 1, observer);

        assert_eq!(first.trace().logical_id, retry.trace().logical_id);
        assert_ne!(first.trace().attempt_id, retry.trace().attempt_id);
        assert_eq!(first.trace().attempt_index, 0);
        assert_eq!(retry.trace().attempt_index, 1);

        first.transition(AttemptState::ChannelQueued).unwrap();
        first
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
        retry.transition(AttemptState::ChannelQueued).unwrap();
        retry
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }

    #[test]
    fn client_timeout_is_orthogonal_to_execution_and_response_delivery() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        attempt.transition(AttemptState::ChannelQueued).unwrap();
        attempt.transition(AttemptState::WaitingGate).unwrap();
        attempt.transition(AttemptState::Running).unwrap();

        attempt.record_client_timeout();
        attempt.finish_execution(ExecutionOutcome::Success).unwrap();
        attempt.record_response_dropped();

        assert_eq!(attempt.state(), AttemptState::ExecutionFinished);
        let events = observer.events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BaselineEvent::ClientTimeout { .. }))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            BaselineEvent::StateTransition {
                next: AttemptState::ExecutionFinished,
                outcome: Some(ExecutionOutcome::Success),
                ..
            }
        )));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BaselineEvent::ResponseDropped { .. }))
        );
    }
}
