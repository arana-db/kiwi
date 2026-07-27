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

use std::cell::Cell;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
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
///
/// State callbacks for one attempt are delivered in transition order. Orthogonal callbacks (for
/// example, execution hooks, timeouts, and response outcomes) may run concurrently with state
/// callbacks and with callbacks from other attempts. Implementations must therefore be
/// concurrency-safe and must not call [`BaselineAttempt`] APIs from inside a callback. A callback
/// panic poisons the shared [`BaselineObserverHandle`].
pub trait BaselineObserver: Send + Sync + 'static {
    fn on_event(&self, event: BaselineEvent);
    fn before_execute(&self, trace: &BaselineTrace);
}

/// Runtime-owned observer state shared by every attempt using one observer.
///
/// Related attempts must reuse the same handle so that a callback panic poisons the observer for
/// the whole related request lifecycle rather than only the attempt that observed the panic.
pub struct BaselineObserverHandle {
    observer: Arc<dyn BaselineObserver>,
    failed: AtomicBool,
}

impl BaselineObserverHandle {
    pub fn new(observer: Arc<dyn BaselineObserver>) -> Arc<Self> {
        Arc::new(Self {
            observer,
            failed: AtomicBool::new(false),
        })
    }

    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn mark_failed(&self) {
        self.failed.store(true, Ordering::Release);
    }
}

/// Reason a lifecycle transition could not complete observably.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BaselineTransitionFailure {
    InvalidTransition,
    ObserverPanicked,
    AttemptPoisoned,
    ReentrantObserver,
}

/// Error returned when a lifecycle transition cannot complete observably.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("baseline attempt transition from {previous:?} to {attempted:?} failed: {reason:?}")]
pub struct BaselineTransitionError {
    previous: AttemptState,
    attempted: AttemptState,
    reason: BaselineTransitionFailure,
}

impl BaselineTransitionError {
    pub fn previous(self) -> AttemptState {
        self.previous
    }

    pub fn attempted(self) -> AttemptState {
        self.attempted
    }

    pub fn reason(self) -> BaselineTransitionFailure {
        self.reason
    }
}

thread_local! {
    static CALLBACK_ACTIVE_ON_THREAD: Cell<bool> = const { Cell::new(false) };
}

struct CallbackScope;

impl CallbackScope {
    fn enter() -> Option<Self> {
        CALLBACK_ACTIVE_ON_THREAD.with(|active| {
            if active.replace(true) {
                None
            } else {
                Some(Self)
            }
        })
    }

    fn is_active() -> bool {
        CALLBACK_ACTIVE_ON_THREAD.with(Cell::get)
    }
}

impl Drop for CallbackScope {
    fn drop(&mut self) {
        CALLBACK_ACTIVE_ON_THREAD.with(|active| active.set(false));
    }
}

struct CallbackActiveGuard<'a> {
    active: &'a AtomicBool,
}

impl<'a> CallbackActiveGuard<'a> {
    fn enter(active: &'a AtomicBool) -> Self {
        let was_active = active.swap(true, Ordering::AcqRel);
        debug_assert!(!was_active);
        Self { active }
    }
}

impl Drop for CallbackActiveGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

struct BaselineAttemptInner {
    trace: BaselineTrace,
    state: AtomicU8,
    pre_accept_disarmed: AtomicBool,
    // `Arc::strong_count` is only a snapshot: concurrent final drops can both
    // observe more than one owner. This counter elects exactly one final token.
    token_count: AtomicUsize,
    started_at: Instant,
    last_transition: Mutex<Instant>,
    dispatch: Mutex<()>,
    callback_active: AtomicBool,
    observer: Arc<BaselineObserverHandle>,
}

/// Cloneable RAII token for one physical storage attempt.
pub struct BaselineAttempt {
    inner: Arc<BaselineAttemptInner>,
}

impl BaselineAttempt {
    pub fn new(
        logical_id: LogicalRequestId,
        attempt_index: u32,
        observer: Arc<BaselineObserverHandle>,
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
                pre_accept_disarmed: AtomicBool::new(false),
                token_count: AtomicUsize::new(1),
                started_at: now,
                last_transition: Mutex::new(now),
                dispatch: Mutex::new(()),
                callback_active: AtomicBool::new(false),
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

    /// Irrevocably reject a physical attempt before the request channel accepts it.
    ///
    /// Returns `Ok(true)` while the attempt is still pre-accept, including repeated
    /// calls, and `Ok(false)` once the attempt has reached `ChannelQueued`. Observer
    /// reentrancy and a previously poisoned observer are returned explicitly.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn disarm_pre_accept(&self) -> Result<bool, BaselineTransitionFailure> {
        if CallbackScope::is_active() || self.inner.callback_active.load(Ordering::Acquire) {
            return Err(BaselineTransitionFailure::ReentrantObserver);
        }
        if self.observer_failed() {
            return Err(BaselineTransitionFailure::AttemptPoisoned);
        }

        let _last_transition = self
            .inner
            .last_transition
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.observer_failed() {
            return Err(BaselineTransitionFailure::AttemptPoisoned);
        }
        if self.state() != AttemptState::Offered {
            return Ok(false);
        }

        self.inner
            .pre_accept_disarmed
            .store(true, Ordering::Release);
        Ok(true)
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
        let trace = self.inner.trace;
        let _ = self.invoke_orthogonal_observer(|observer| observer.before_execute(&trace));
    }

    pub fn record_client_timeout(&self) {
        let event = BaselineEvent::ClientTimeout {
            trace: self.inner.trace,
            elapsed: self.inner.started_at.elapsed(),
        };
        let _ = self.invoke_orthogonal_observer(move |observer| observer.on_event(event));
    }

    pub fn record_response_dropped(&self) {
        let event = BaselineEvent::ResponseDropped {
            trace: self.inner.trace,
            elapsed: self.inner.started_at.elapsed(),
        };
        let _ = self.invoke_orthogonal_observer(move |observer| observer.on_event(event));
    }

    pub fn record_response_delivered(&self) {
        let event = BaselineEvent::ResponseDelivered {
            trace: self.inner.trace,
            elapsed: self.inner.started_at.elapsed(),
        };
        let _ = self.invoke_orthogonal_observer(move |observer| observer.on_event(event));
    }

    fn transition_inner(
        &self,
        next: AttemptState,
        outcome: Option<ExecutionOutcome>,
    ) -> Result<(), BaselineTransitionError> {
        if CallbackScope::is_active() || self.inner.callback_active.load(Ordering::Acquire) {
            return Err(self.transition_error(next, BaselineTransitionFailure::ReentrantObserver));
        }
        if self.observer_failed() {
            return Err(self.transition_error(next, BaselineTransitionFailure::AttemptPoisoned));
        }

        let _dispatch = self
            .inner
            .dispatch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.observer_failed() {
            return Err(self.transition_error(next, BaselineTransitionFailure::AttemptPoisoned));
        }

        let (previous, event, valid) = {
            let mut last_transition = self
                .inner
                .last_transition
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = self.state();
            if (previous == AttemptState::Offered
                && self.inner.pre_accept_disarmed.load(Ordering::Acquire))
                || !previous.permits(next)
                || (next == AttemptState::ExecutionFinished && outcome.is_none())
                || (next != AttemptState::ExecutionFinished && outcome.is_some())
            {
                let event = BaselineEvent::InvariantViolation {
                    trace: self.inner.trace,
                    previous,
                    attempted: next,
                    elapsed: self.inner.started_at.elapsed(),
                };
                (previous, event, false)
            } else {
                self.inner.state.store(next as u8, Ordering::Release);
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(*last_transition);
                *last_transition = now;
                let event = BaselineEvent::StateTransition {
                    trace: self.inner.trace,
                    previous,
                    next,
                    elapsed,
                    outcome,
                };
                (previous, event, true)
            }
        };

        let callback_result = self.invoke_observer(move |observer| observer.on_event(event));
        match callback_result {
            Ok(()) if valid => Ok(()),
            Ok(()) => Err(BaselineTransitionError {
                previous,
                attempted: next,
                reason: BaselineTransitionFailure::InvalidTransition,
            }),
            Err(reason) => Err(BaselineTransitionError {
                previous,
                attempted: next,
                reason,
            }),
        }
    }

    fn transition_error(
        &self,
        attempted: AttemptState,
        reason: BaselineTransitionFailure,
    ) -> BaselineTransitionError {
        BaselineTransitionError {
            previous: self.state(),
            attempted,
            reason,
        }
    }

    fn observer_failed(&self) -> bool {
        self.inner.observer.failed()
    }

    fn invoke_orthogonal_observer<F>(&self, callback: F) -> Result<(), BaselineTransitionFailure>
    where
        F: FnOnce(&dyn BaselineObserver),
    {
        if CallbackScope::is_active() {
            self.inner.observer.mark_failed();
            return Err(BaselineTransitionFailure::ReentrantObserver);
        }
        if self.observer_failed() {
            return Err(BaselineTransitionFailure::AttemptPoisoned);
        }
        let Some(_scope) = CallbackScope::enter() else {
            self.inner.observer.mark_failed();
            return Err(BaselineTransitionFailure::ReentrantObserver);
        };

        let result = catch_unwind(AssertUnwindSafe(|| {
            callback(self.inner.observer.observer.as_ref())
        }));
        if result.is_err() {
            self.inner.observer.mark_failed();
            return Err(BaselineTransitionFailure::ObserverPanicked);
        }
        Ok(())
    }

    fn invoke_observer<F>(&self, callback: F) -> Result<(), BaselineTransitionFailure>
    where
        F: FnOnce(&dyn BaselineObserver),
    {
        if self.observer_failed() {
            return Err(BaselineTransitionFailure::AttemptPoisoned);
        }
        let Some(_scope) = CallbackScope::enter() else {
            return Err(BaselineTransitionFailure::ReentrantObserver);
        };
        let _active = CallbackActiveGuard::enter(&self.inner.callback_active);

        let result = catch_unwind(AssertUnwindSafe(|| {
            callback(self.inner.observer.observer.as_ref())
        }));
        if result.is_err() {
            self.inner.observer.mark_failed();
            return Err(BaselineTransitionFailure::ObserverPanicked);
        }
        Ok(())
    }
}

impl Clone for BaselineAttempt {
    fn clone(&self) -> Self {
        let inner = Arc::clone(&self.inner);
        self.inner.token_count.fetch_add(1, Ordering::Relaxed);
        Self { inner }
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
        if self.inner.token_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        let _dispatch = self
            .inner
            .dispatch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let transition = {
            let mut last_transition = self
                .inner
                .last_transition
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = self.state();
            if previous == AttemptState::Offered || previous.is_terminal() {
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
            let event = BaselineEvent::StateTransition {
                trace: self.inner.trace,
                previous,
                next: AttemptState::Abandoned,
                elapsed,
                outcome: None,
            };
            if matches!(
                self.invoke_observer(move |observer| observer.on_event(event)),
                Err(BaselineTransitionFailure::ReentrantObserver)
            ) {
                self.inner.observer.mark_failed();
            }
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
    use std::thread;

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

    struct BlockingOrderObserver {
        events: Mutex<Vec<AttemptState>>,
        first_entered: mpsc::Sender<()>,
        second_entered: mpsc::Sender<()>,
        release_first: Arc<(Mutex<bool>, Condvar)>,
    }

    impl BaselineObserver for BlockingOrderObserver {
        fn on_event(&self, event: BaselineEvent) {
            let BaselineEvent::StateTransition { next, .. } = event else {
                return;
            };

            if next == AttemptState::ChannelQueued {
                self.first_entered
                    .send(())
                    .expect("first callback receiver must remain open");
                let (released, release_ready) = &*self.release_first;
                let mut released = released
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                while !*released {
                    released = release_ready
                        .wait(released)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }

            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(next);
            if next == AttemptState::WaitingGate {
                self.second_entered
                    .send(())
                    .expect("second callback receiver must remain open");
            }
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    struct PanicOnEventObserver;

    impl BaselineObserver for PanicOnEventObserver {
        fn on_event(&self, _event: BaselineEvent) {
            panic!("observer event panic");
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    #[derive(Default)]
    struct ReentrantObserver {
        attempt: Mutex<Option<BaselineAttempt>>,
        result: Mutex<Option<Result<(), BaselineTransitionError>>>,
    }

    #[derive(Default)]
    struct SpawnThreadReentrantObserver {
        attempt: Mutex<Option<BaselineAttempt>>,
        child: Mutex<Option<thread::JoinHandle<Result<(), BaselineTransitionError>>>>,
        timed_out: AtomicBool,
    }

    #[derive(Default)]
    struct CrossAttemptObserver {
        target: Mutex<Option<BaselineAttempt>>,
        result: Mutex<Option<Result<(), BaselineTransitionError>>>,
    }

    #[derive(Default)]
    struct CrossAttemptDisarmObserver {
        target: Mutex<Option<BaselineAttempt>>,
        result: Mutex<Option<Result<bool, BaselineTransitionFailure>>>,
    }

    impl BaselineObserver for CrossAttemptDisarmObserver {
        fn on_event(&self, event: BaselineEvent) {
            if !matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::ChannelQueued,
                    ..
                }
            ) {
                return;
            }
            let Some(target) = self
                .target
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            else {
                return;
            };
            let result = target.disarm_pre_accept();
            *self
                .result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    impl BaselineObserver for CrossAttemptObserver {
        fn on_event(&self, event: BaselineEvent) {
            if !matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::ChannelQueued,
                    ..
                }
            ) {
                return;
            }
            let Some(target) = self
                .target
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            else {
                return;
            };
            let result = target.transition(AttemptState::ChannelQueued);
            *self
                .result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    impl BaselineObserver for SpawnThreadReentrantObserver {
        fn on_event(&self, event: BaselineEvent) {
            if !matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::ChannelQueued,
                    ..
                }
            ) {
                return;
            }
            let attempt = self
                .attempt
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .expect("spawn-thread reentrant attempt must be installed");
            let (result_sender, result_receiver) = mpsc::channel();
            let child = thread::spawn(move || {
                let result = attempt.transition(AttemptState::WaitingGate);
                let _ = result_sender.send(result);
                result
            });
            *self
                .child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(child);
            if result_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_err()
            {
                self.timed_out.store(true, Ordering::Release);
            }
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    impl BaselineObserver for ReentrantObserver {
        fn on_event(&self, event: BaselineEvent) {
            if !matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::ChannelQueued,
                    ..
                }
            ) {
                return;
            }
            let attempt = self
                .attempt
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .expect("reentrant test attempt must be installed");
            let result = attempt.transition(AttemptState::WaitingGate);
            *self
                .result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    struct PanicBeforeExecuteObserver;

    impl BaselineObserver for PanicBeforeExecuteObserver {
        fn on_event(&self, _event: BaselineEvent) {}

        fn before_execute(&self, _trace: &BaselineTrace) {
            panic!("observer before_execute panic");
        }
    }

    struct PanicOnAbandonedObserver;

    impl BaselineObserver for PanicOnAbandonedObserver {
        fn on_event(&self, event: BaselineEvent) {
            if matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::Abandoned,
                    ..
                }
            ) {
                panic!("observer abandoned panic");
            }
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    #[derive(Clone, Copy)]
    enum BlockedCallback {
        Timeout,
        Running,
    }

    struct OrthogonalOverlapObserver {
        events: Mutex<Vec<BaselineEvent>>,
        blocked: BlockedCallback,
        entered: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl BaselineObserver for OrthogonalOverlapObserver {
        fn on_event(&self, event: BaselineEvent) {
            let should_block = matches!(
                (&self.blocked, &event),
                (
                    BlockedCallback::Timeout,
                    BaselineEvent::ClientTimeout { .. }
                ) | (
                    BlockedCallback::Running,
                    BaselineEvent::StateTransition {
                        next: AttemptState::Running,
                        ..
                    }
                )
            );
            if should_block {
                self.entered
                    .send(())
                    .expect("blocked callback receiver must remain open");
                let (released, release_ready) = &*self.release;
                let mut released = released
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                while !*released {
                    released = release_ready
                        .wait(released)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event);
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    struct BlockingBeforeExecuteObserver {
        entered: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        timeout_seen: AtomicBool,
    }

    impl BaselineObserver for BlockingBeforeExecuteObserver {
        fn on_event(&self, event: BaselineEvent) {
            if matches!(event, BaselineEvent::ClientTimeout { .. }) {
                self.timeout_seen.store(true, Ordering::Release);
            }
        }

        fn before_execute(&self, _trace: &BaselineTrace) {
            self.entered
                .send(())
                .expect("before_execute receiver must remain open");
            let (released, release_ready) = &*self.release;
            let mut released = released
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*released {
                released = release_ready
                    .wait(released)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }

    #[derive(Default)]
    struct ReentrantVoidObserver {
        attempt: Mutex<Option<BaselineAttempt>>,
    }

    impl BaselineObserver for ReentrantVoidObserver {
        fn on_event(&self, event: BaselineEvent) {
            if matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::ChannelQueued,
                    ..
                }
            ) {
                self.attempt
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                    .expect("reentrant void attempt must be installed")
                    .record_client_timeout();
            }
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    #[derive(Default)]
    struct DropAcceptedAttemptObserver {
        target: Mutex<Option<BaselineAttempt>>,
    }

    impl BaselineObserver for DropAcceptedAttemptObserver {
        fn on_event(&self, event: BaselineEvent) {
            if matches!(
                event,
                BaselineEvent::StateTransition {
                    next: AttemptState::ChannelQueued,
                    ..
                }
            ) {
                drop(
                    self.target
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                        .expect("accepted drop target must be installed"),
                );
            }
        }

        fn before_execute(&self, _trace: &BaselineTrace) {}
    }

    fn release_blocked_callback(release: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, release_ready) = &**release;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        release_ready.notify_all();
    }

    fn attempt(observer: Arc<RecordingObserver>) -> BaselineAttempt {
        BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(observer),
        )
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
    fn transition_during_callback_fails_fast_then_retry_preserves_order() {
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let release_first = Arc::new((Mutex::new(false), Condvar::new()));
        let observer = Arc::new(BlockingOrderObserver {
            events: Mutex::new(Vec::new()),
            first_entered: first_entered_tx,
            second_entered: second_entered_tx,
            release_first: release_first.clone(),
        });
        let attempt = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(observer.clone()),
        );
        let first = attempt.clone();
        let first_transition = thread::spawn(move || first.transition(AttemptState::ChannelQueued));
        first_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first callback must enter");

        let second = attempt.clone();
        let (second_result_sender, second_result_receiver) = mpsc::channel();
        let second_transition = thread::spawn(move || {
            let result = second.transition(AttemptState::WaitingGate);
            let _ = second_result_sender.send(result);
            result
        });
        let second_result = second_result_receiver.recv_timeout(Duration::from_secs(1));

        let (released, release_ready) = &*release_first;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        release_ready.notify_all();

        first_transition
            .join()
            .expect("first transition thread must not panic")
            .expect("first transition must succeed");
        let joined_second = second_transition
            .join()
            .expect("second transition thread must not panic");

        let error = second_result
            .expect("transition during callback must fail fast")
            .expect_err("transition during callback must be rejected");
        assert_eq!(error.reason(), BaselineTransitionFailure::ReentrantObserver);
        assert_eq!(joined_second, Err(error));

        attempt.transition(AttemptState::WaitingGate).unwrap();
        second_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("retry callback must run after the first callback completes");
        assert_eq!(
            observer
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[AttemptState::ChannelQueued, AttemptState::WaitingGate]
        );
    }

    #[test]
    fn observer_callback_panic_is_contained_and_reported() {
        let observer = BaselineObserverHandle::new(Arc::new(PanicOnEventObserver));
        let attempt = BaselineAttempt::new(LogicalRequestId::new(), 0, observer.clone());

        let result = catch_unwind(AssertUnwindSafe(|| {
            attempt.transition(AttemptState::ChannelQueued)
        }));

        assert!(result.is_ok(), "observer panic must not escape runtime");
        let error = result
            .expect("observer panic must be converted into a transition error")
            .expect_err("observer panic must fail the transition observably");
        assert_eq!(error.reason(), BaselineTransitionFailure::ObserverPanicked);
        assert!(observer.failed());

        let poisoned = attempt
            .transition(AttemptState::WaitingGate)
            .expect_err("the same attempt must fail closed after observer panic");
        assert_eq!(
            poisoned.reason(),
            BaselineTransitionFailure::AttemptPoisoned
        );

        let shared_observer_attempt =
            BaselineAttempt::new(LogicalRequestId::new(), 1, observer.clone());
        let shared_poisoned = shared_observer_attempt
            .transition(AttemptState::ChannelQueued)
            .expect_err("all attempts sharing the observer must fail closed");
        assert_eq!(
            shared_poisoned.reason(),
            BaselineTransitionFailure::AttemptPoisoned
        );
    }

    #[test]
    fn observer_reentrant_transition_returns_error_without_deadlock() {
        let observer = Arc::new(ReentrantObserver::default());
        let observer_handle = BaselineObserverHandle::new(observer.clone());
        let attempt = BaselineAttempt::new(LogicalRequestId::new(), 0, observer_handle.clone());
        *observer
            .attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(attempt.clone());

        attempt.transition(AttemptState::ChannelQueued).unwrap();

        let error = observer
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("observer must record the reentrant result")
            .expect_err("reentrant transition must be rejected");
        assert_eq!(error.reason(), BaselineTransitionFailure::ReentrantObserver);
        assert_eq!(attempt.state(), AttemptState::ChannelQueued);
        assert!(!observer_handle.failed());
        attempt
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }

    #[test]
    fn observer_spawned_thread_reentrant_transition_fails_without_deadlock() {
        let observer = Arc::new(SpawnThreadReentrantObserver::default());
        let attempt = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(observer.clone()),
        );
        *observer
            .attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(attempt.clone());

        attempt.transition(AttemptState::ChannelQueued).unwrap();

        let child_result = observer
            .child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("observer must store the child thread")
            .join()
            .expect("observer child thread must not panic");
        assert!(
            !observer.timed_out.load(Ordering::Acquire),
            "child transition must fail fast while its parent callback is active"
        );
        let error = child_result.expect_err("cross-thread reentrant transition must be rejected");
        assert_eq!(error.reason(), BaselineTransitionFailure::ReentrantObserver);
        assert_eq!(attempt.state(), AttemptState::ChannelQueued);
        attempt
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }

    #[test]
    fn observer_cross_attempt_transition_is_rejected_before_dispatch() {
        let first_observer = Arc::new(CrossAttemptObserver::default());
        let second_observer = Arc::new(CrossAttemptObserver::default());
        let first = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(first_observer.clone()),
        );
        let second = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(second_observer),
        );
        *first_observer
            .target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(second.clone());

        first.transition(AttemptState::ChannelQueued).unwrap();

        let error = first_observer
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("cross-attempt callback must record its result")
            .expect_err("callback must not enter another attempt dispatch");
        assert_eq!(error.reason(), BaselineTransitionFailure::ReentrantObserver);
        assert_eq!(second.state(), AttemptState::Offered);
        first
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }

    #[test]
    fn observer_cross_attempt_disarm_is_rejected_without_mutation() {
        let target_observer = Arc::new(RecordingObserver::default());
        let target_handle = BaselineObserverHandle::new(target_observer);
        let target = BaselineAttempt::new(LogicalRequestId::new(), 0, target_handle.clone());

        let source_observer = Arc::new(CrossAttemptDisarmObserver::default());
        *source_observer
            .target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(target.clone());
        let source = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(source_observer.clone()),
        );

        source.transition(AttemptState::ChannelQueued).unwrap();

        let error = source_observer
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("observer must record the cross-attempt disarm result")
            .expect_err("observer callback must not disarm another attempt");
        assert_eq!(error, BaselineTransitionFailure::ReentrantObserver);
        assert_eq!(target.state(), AttemptState::Offered);
        assert!(!target_handle.failed());
        target.transition(AttemptState::ChannelQueued).unwrap();
        target
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
        source
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }

    #[test]
    fn poisoned_observer_rejects_pre_accept_disarm() {
        let observer = BaselineObserverHandle::new(Arc::new(PanicBeforeExecuteObserver));
        let attempt = BaselineAttempt::new(LogicalRequestId::new(), 0, observer.clone());
        attempt.before_execute();

        let error = attempt
            .disarm_pre_accept()
            .expect_err("poisoned observer must reject pre-accept disarm");

        assert_eq!(error, BaselineTransitionFailure::AttemptPoisoned);
        assert!(observer.failed());
        assert_eq!(attempt.state(), AttemptState::Offered);
    }

    #[test]
    fn before_execute_panic_is_contained_and_poison_is_visible() {
        let observer = BaselineObserverHandle::new(Arc::new(PanicBeforeExecuteObserver));
        let attempt = BaselineAttempt::new(LogicalRequestId::new(), 0, observer.clone());

        let result = catch_unwind(AssertUnwindSafe(|| attempt.before_execute()));

        assert!(
            result.is_ok(),
            "before_execute panic must not escape runtime"
        );
        assert!(observer.failed());
        let error = attempt
            .transition(AttemptState::ChannelQueued)
            .expect_err("observer poison must reject later mutation");
        assert_eq!(error.reason(), BaselineTransitionFailure::AttemptPoisoned);
    }

    #[test]
    fn drop_callback_panic_during_unwind_does_not_abort_process() {
        const CHILD_ENV: &str = "KIWI_BASELINE_DROP_PANIC_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            let observer = BaselineObserverHandle::new(Arc::new(PanicOnAbandonedObserver));
            let unwind = catch_unwind(AssertUnwindSafe({
                let observer = observer.clone();
                move || {
                    let attempt =
                        BaselineAttempt::new(LogicalRequestId::new(), 0, observer.clone());
                    attempt.transition(AttemptState::ChannelQueued).unwrap();
                    panic!("outer panic");
                }
            }));
            assert!(unwind.is_err(), "outer panic must remain observable");
            assert!(
                observer.failed(),
                "drop callback panic must set the shared failure flag"
            );
            return;
        }

        let output = Command::new(std::env::current_exe().expect("test executable must exist"))
            .arg("--exact")
            .arg("baseline::tests::drop_callback_panic_during_unwind_does_not_abort_process")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .output()
            .expect("child test process must start");
        assert!(
            output.status.success(),
            "child must not abort: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr)
                .contains("panic in a destructor during cleanup")
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
    fn concurrent_final_clone_drops_emit_exactly_one_abandoned_event() {
        const TOKEN_COUNT: usize = 8;

        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        attempt.transition(AttemptState::ChannelQueued).unwrap();
        let mut tokens = (1..TOKEN_COUNT)
            .map(|_| attempt.clone())
            .collect::<Vec<_>>();
        tokens.push(attempt);
        let barrier = Arc::new(Barrier::new(TOKEN_COUNT));

        let drops = tokens
            .into_iter()
            .map(|token| {
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    drop(token);
                })
            })
            .collect::<Vec<_>>();
        for drop_thread in drops {
            drop_thread
                .join()
                .expect("concurrent token drop must not panic");
        }

        assert_eq!(
            observer
                .events()
                .iter()
                .filter(|event| matches!(
                    event,
                    BaselineEvent::StateTransition {
                        next: AttemptState::Abandoned,
                        ..
                    }
                ))
                .count(),
            1
        );
    }

    #[test]
    fn pre_accept_disarm_is_idempotent_and_drop_emits_no_terminal_event() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());

        assert!(attempt.disarm_pre_accept().unwrap());
        assert!(attempt.disarm_pre_accept().unwrap());
        drop(attempt);

        assert!(observer.events().is_empty());
    }

    #[test]
    fn disarmed_pre_accept_attempt_cannot_later_be_accepted() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        assert!(attempt.disarm_pre_accept().unwrap());

        let error = attempt.transition(AttemptState::ChannelQueued).unwrap_err();

        assert_eq!(error.previous(), AttemptState::Offered);
        assert_eq!(error.attempted(), AttemptState::ChannelQueued);
        assert_eq!(attempt.state(), AttemptState::Offered);
        drop(attempt);
        assert!(!observer.events().iter().any(|event| matches!(
            event,
            BaselineEvent::StateTransition {
                next: AttemptState::ExecutionFinished
                    | AttemptState::ShutdownRejectedAfterAccept
                    | AttemptState::Abandoned,
                ..
            }
        )));
    }

    #[test]
    fn pre_accept_disarm_cannot_mask_an_accepted_attempt() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        attempt.transition(AttemptState::ChannelQueued).unwrap();

        assert!(!attempt.disarm_pre_accept().unwrap());
        drop(attempt);

        assert_eq!(
            observer
                .events()
                .iter()
                .filter(|event| matches!(
                    event,
                    BaselineEvent::StateTransition {
                        next: AttemptState::Abandoned,
                        ..
                    }
                ))
                .count(),
            1
        );
    }

    #[test]
    fn disarm_and_accept_race_has_one_linearized_outcome() {
        let observer = Arc::new(RecordingObserver::default());
        let attempt = attempt(observer.clone());
        let barrier = Arc::new(Barrier::new(3));

        let disarm_attempt = attempt.clone();
        let disarm_barrier = barrier.clone();
        let disarm = thread::spawn(move || {
            disarm_barrier.wait();
            disarm_attempt.disarm_pre_accept()
        });
        let accept_attempt = attempt.clone();
        let accept_barrier = barrier.clone();
        let accept = thread::spawn(move || {
            accept_barrier.wait();
            accept_attempt.transition(AttemptState::ChannelQueued)
        });
        barrier.wait();

        let disarmed = disarm.join().expect("disarm thread must not panic");
        let accepted = accept.join().expect("accept thread must not panic");
        match (disarmed, accepted) {
            (Ok(true), Err(error)) => {
                assert_eq!(error.reason(), BaselineTransitionFailure::InvalidTransition);
            }
            (Ok(false), Ok(())) => {}
            outcome => panic!("race must linearize to disarmed or accepted: {outcome:?}"),
        }
        drop(attempt);

        let events = observer.events();
        let accepted_count = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    BaselineEvent::StateTransition {
                        previous: AttemptState::Offered,
                        next: AttemptState::ChannelQueued,
                        ..
                    }
                )
            })
            .count();
        let abandoned_count = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    BaselineEvent::StateTransition {
                        next: AttemptState::Abandoned,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(accepted_count, abandoned_count);
        assert_eq!(
            accepted_count,
            usize::from(!disarmed.expect("disarm race must not fail"))
        );
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
        let observer = BaselineObserverHandle::new(observer);
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

    #[test]
    fn execution_can_finish_while_timeout_callback_is_active() {
        let (entered_sender, entered_receiver) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let observer = Arc::new(OrthogonalOverlapObserver {
            events: Mutex::new(Vec::new()),
            blocked: BlockedCallback::Timeout,
            entered: entered_sender,
            release: release.clone(),
        });
        let attempt = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(observer.clone()),
        );
        attempt.transition(AttemptState::ChannelQueued).unwrap();
        attempt.transition(AttemptState::WaitingGate).unwrap();
        attempt.transition(AttemptState::Running).unwrap();

        let timeout_attempt = attempt.clone();
        let timeout_thread = thread::spawn(move || timeout_attempt.record_client_timeout());
        entered_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("timeout callback must enter");

        let (finish_started_sender, finish_started_receiver) = mpsc::channel();
        let (finish_completion_sender, finish_completion_receiver) = mpsc::channel();
        let finish_attempt = attempt.clone();
        let finish_thread = thread::spawn(move || {
            finish_started_sender
                .send(())
                .expect("finish start receiver must remain alive");
            let result = finish_attempt.finish_execution(ExecutionOutcome::Success);
            finish_completion_sender
                .send(result)
                .expect("finish completion receiver must remain alive");
        });
        finish_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("finish thread must start");
        let finish_completion = finish_completion_receiver.recv_timeout(Duration::from_secs(1));

        release_blocked_callback(&release);
        timeout_thread
            .join()
            .expect("timeout callback thread must not panic");
        finish_thread.join().expect("finish thread must not panic");

        finish_completion
            .expect("execution finish must complete while timeout callback is blocked")
            .expect("execution finish must not be rejected by timeout callback overlap");
        let events = observer
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BaselineEvent::ClientTimeout { .. }))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            BaselineEvent::StateTransition {
                next: AttemptState::ExecutionFinished,
                ..
            }
        )));
    }

    #[test]
    fn timeout_is_recorded_while_state_callback_is_active() {
        let (entered_sender, entered_receiver) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let observer = Arc::new(OrthogonalOverlapObserver {
            events: Mutex::new(Vec::new()),
            blocked: BlockedCallback::Running,
            entered: entered_sender,
            release: release.clone(),
        });
        let attempt = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(observer.clone()),
        );
        attempt.transition(AttemptState::ChannelQueued).unwrap();
        attempt.transition(AttemptState::WaitingGate).unwrap();

        let running_attempt = attempt.clone();
        let running_thread =
            thread::spawn(move || running_attempt.transition(AttemptState::Running));
        entered_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("running callback must enter");

        let (timeout_started_sender, timeout_started_receiver) = mpsc::channel();
        let (timeout_completion_sender, timeout_completion_receiver) = mpsc::channel();
        let timeout_attempt = attempt.clone();
        let timeout_thread = thread::spawn(move || {
            timeout_started_sender
                .send(())
                .expect("timeout start receiver must remain alive");
            timeout_attempt.record_client_timeout();
            timeout_completion_sender
                .send(())
                .expect("timeout completion receiver must remain alive");
        });
        timeout_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("timeout thread must start");
        let timeout_completion = timeout_completion_receiver.recv_timeout(Duration::from_secs(1));

        release_blocked_callback(&release);
        running_thread
            .join()
            .expect("running transition thread must not panic")
            .expect("running transition must succeed");
        timeout_thread
            .join()
            .expect("timeout callback thread must not panic");
        timeout_completion.expect("timeout callback must complete while state callback is blocked");
        attempt.finish_execution(ExecutionOutcome::Success).unwrap();

        let events = observer
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BaselineEvent::ClientTimeout { .. })),
            "timeout event must not be silently dropped during state callback"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            BaselineEvent::StateTransition {
                next: AttemptState::ExecutionFinished,
                ..
            }
        )));
    }

    #[test]
    fn timeout_can_overlap_blocking_before_execute_without_poisoning_observer() {
        let (entered_sender, entered_receiver) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let observer = Arc::new(BlockingBeforeExecuteObserver {
            entered: entered_sender,
            release: release.clone(),
            timeout_seen: AtomicBool::new(false),
        });
        let observer_handle = BaselineObserverHandle::new(observer.clone());
        let attempt = BaselineAttempt::new(LogicalRequestId::new(), 0, observer_handle.clone());

        let before_attempt = attempt.clone();
        let before_thread = thread::spawn(move || before_attempt.before_execute());
        entered_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("before_execute callback must enter");

        let (timeout_started_sender, timeout_started_receiver) = mpsc::channel();
        let (timeout_completion_sender, timeout_completion_receiver) = mpsc::channel();
        let timeout_attempt = attempt.clone();
        let timeout_thread = thread::spawn(move || {
            timeout_started_sender
                .send(())
                .expect("timeout start receiver must remain alive");
            timeout_attempt.record_client_timeout();
            timeout_completion_sender
                .send(())
                .expect("timeout completion receiver must remain alive");
        });
        timeout_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("timeout thread must start");
        let timeout_completion = timeout_completion_receiver.recv_timeout(Duration::from_secs(1));

        release_blocked_callback(&release);
        before_thread
            .join()
            .expect("before_execute callback thread must not panic");
        timeout_thread
            .join()
            .expect("timeout callback thread must not panic");
        timeout_completion
            .expect("timeout callback must complete while before_execute callback is blocked");
        assert!(observer.timeout_seen.load(Ordering::Acquire));
        assert!(!observer_handle.failed());
    }

    #[test]
    fn same_thread_reentrant_void_callback_marks_observer_failed() {
        let observer = Arc::new(ReentrantVoidObserver::default());
        let observer_handle = BaselineObserverHandle::new(observer.clone());
        let attempt = BaselineAttempt::new(LogicalRequestId::new(), 0, observer_handle.clone());
        *observer
            .attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(attempt.clone());

        attempt.transition(AttemptState::ChannelQueued).unwrap();

        assert!(observer_handle.failed());
        assert_eq!(attempt.state(), AttemptState::ChannelQueued);
    }

    #[test]
    fn reentrant_drop_terminalizes_state_and_marks_target_observer_failed() {
        let target_observer = Arc::new(RecordingObserver::default());
        let target_handle = BaselineObserverHandle::new(target_observer.clone());
        let target = BaselineAttempt::new(LogicalRequestId::new(), 0, target_handle.clone());
        target.transition(AttemptState::ChannelQueued).unwrap();
        let target_inner = target.inner.clone();

        let source_observer = Arc::new(DropAcceptedAttemptObserver::default());
        *source_observer
            .target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(target);
        let source = BaselineAttempt::new(
            LogicalRequestId::new(),
            0,
            BaselineObserverHandle::new(source_observer),
        );

        source.transition(AttemptState::ChannelQueued).unwrap();

        assert_eq!(
            AttemptState::from_u8(target_inner.state.load(Ordering::Acquire)),
            AttemptState::Abandoned
        );
        assert!(target_handle.failed());
        assert!(!target_observer.events().iter().any(|event| matches!(
            event,
            BaselineEvent::StateTransition {
                next: AttemptState::Abandoned,
                ..
            }
        )));
        source
            .transition(AttemptState::ShutdownRejectedAfterAccept)
            .unwrap();
    }
}
