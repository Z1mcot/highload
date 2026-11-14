#![allow(dead_code)]
use std::collections::VecDeque;
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use resilience::{LatencyBudget, Response, RetryBudget, ServerOutcome, SubrequestSender};
use tokio::time::{sleep, Instant};

pub fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

pub fn latency(overall: u64, per_attempt: u64) -> LatencyBudget {
    LatencyBudget {
        overall: millis(overall),
        per_attempt: millis(per_attempt),
    }
}

pub fn retry_budget(max_attempts: usize) -> RetryBudget {
    RetryBudget {
        max_attempts,
        max_client_errors: max_attempts,
        max_server_errors: max_attempts,
        max_timeouts: max_attempts,
    }
}

pub fn success(body: &str) -> ServerOutcome {
    ServerOutcome::Success(Response {
        body: body.as_bytes().to_vec(),
    })
}

pub enum Step {
    Respond(Duration, ServerOutcome),
    Hang,
}

pub fn immediate(outcome: ServerOutcome) -> Step {
    Step::Respond(Duration::ZERO, outcome)
}

#[derive(Debug, Clone)]
pub struct Call {
    pub server_index: usize,
    pub started_at: Instant,
}

#[derive(Default)]
pub struct Trace {
    pub calls: Mutex<Vec<Call>>,
    pub completed: Mutex<usize>,
    pub canceled: Mutex<usize>,
    pub active: Mutex<usize>,
}

impl Trace {
    pub fn order(&self) -> Vec<usize> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.server_index)
            .collect()
    }

    pub fn times(&self) -> Vec<Instant> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.started_at)
            .collect()
    }
}

pub struct ScriptedSender {
    server_index: usize,
    steps: Mutex<VecDeque<Step>>,
    pub trace: Arc<Trace>,
}

impl ScriptedSender {
    pub fn new(steps: Vec<Step>) -> Self {
        Self::with_trace(0, steps, Arc::new(Trace::default()))
    }

    pub fn with_trace(server_index: usize, steps: Vec<Step>, trace: Arc<Trace>) -> Self {
        Self {
            server_index,
            steps: Mutex::new(steps.into()),
            trace,
        }
    }
}

pub fn sender_list(scripts: Vec<Vec<Step>>, trace: &Arc<Trace>) -> Vec<Arc<dyn SubrequestSender>> {
    scripts
        .into_iter()
        .enumerate()
        .map(|(server_index, steps)| {
            Arc::new(ScriptedSender::with_trace(
                server_index,
                steps,
                trace.clone(),
            )) as Arc<dyn SubrequestSender>
        })
        .collect()
}

struct InFlight {
    trace: Arc<Trace>,
    completed: bool,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        *self.trace.active.lock().unwrap() -= 1;
        if !self.completed {
            *self.trace.canceled.lock().unwrap() += 1;
        }
    }
}

#[async_trait]
impl SubrequestSender for ScriptedSender {
    async fn send(&self) -> ServerOutcome {
        self.trace.calls.lock().unwrap().push(Call {
            server_index: self.server_index,
            started_at: Instant::now(),
        });
        *self.trace.active.lock().unwrap() += 1;
        let mut in_flight = InFlight {
            trace: self.trace.clone(),
            completed: false,
        };
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra sender call");
        let outcome = match step {
            Step::Respond(delay, outcome) => {
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                outcome
            }
            Step::Hang => pending().await,
        };
        in_flight.completed = true;
        *self.trace.completed.lock().unwrap() += 1;
        outcome
    }
}
