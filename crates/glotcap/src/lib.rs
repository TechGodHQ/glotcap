//! Pure, bounded session rules; no clocks, providers, transports or runtime.
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub replay: usize,
    pub max_bytes: usize,
    pub events: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            replay: 64,
            max_bytes: 32000 * 60,
            events: 128,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Draining,
    Completed,
    Cancelled,
    Failed,
}
impl Status {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Receipt {
    pub next_sequence: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Event {
    pub event_id: u32,
    pub kind: String,
    pub segment_id: Option<u32>,
    pub revision: Option<u32>,
    pub text: Option<String>,
    pub is_final: bool,
}
pub struct Session {
    pub status: Status,
    pub next_sequence: u32,
    limits: Limits,
    bytes: usize,
    replay: VecDeque<(u32, Vec<u8>, Receipt)>,
    events: VecDeque<Event>,
    last_event: u32,
    revision: u32,
}
impl Session {
    pub fn new(limits: Limits) -> Self {
        assert!(limits.replay > 0 && limits.events >= 2);
        Self {
            status: Status::Open,
            next_sequence: 0,
            limits,
            bytes: 0,
            replay: VecDeque::new(),
            events: VecDeque::new(),
            last_event: 0,
            revision: 0,
        }
    }
    /// Returns the original receipt, not a new receipt at the current high watermark.
    pub fn replay(&self, sequence: u32, bytes: &[u8]) -> Result<Option<Receipt>, &'static str> {
        if sequence >= self.next_sequence {
            return Ok(None);
        }
        let (_, prior, receipt) = self
            .replay
            .iter()
            .find(|(s, _, _)| *s == sequence)
            .ok_or("replay_expired")?;
        if prior != bytes {
            return Err("replay_conflict");
        }
        Ok(Some(receipt.clone()))
    }
    pub fn append(&mut self, sequence: u32, bytes: Vec<u8>) -> Result<Receipt, &'static str> {
        if bytes.is_empty() || bytes.len() > 3200 || !bytes.len().is_multiple_of(2) {
            return Err("invalid_pcm");
        }
        if let Some(receipt) = self.replay(sequence, &bytes)? {
            return Ok(receipt);
        }
        if self.status != Status::Open {
            return Err("ingress_closed");
        }
        if sequence != self.next_sequence {
            return Err("sequence_gap");
        }
        if bytes.len() > self.limits.max_bytes.saturating_sub(self.bytes) {
            return Err("duration_limit");
        }
        self.bytes += bytes.len();
        self.next_sequence = self.next_sequence.checked_add(1).ok_or("sequence_limit")?;
        let receipt = Receipt {
            next_sequence: self.next_sequence,
        };
        self.replay.push_back((sequence, bytes, receipt.clone()));
        while self.replay.len() > self.limits.replay {
            self.replay.pop_front();
        }
        Ok(receipt)
    }
    pub fn finish(&mut self) -> Result<(), &'static str> {
        match self.status {
            Status::Open => {
                self.status = Status::Draining;
                Ok(())
            }
            Status::Draining | Status::Completed => Ok(()),
            _ => Err("terminal_conflict"),
        }
    }
    fn emit(&mut self, kind: &str, text: Option<String>, final_text: bool) {
        self.last_event += 1;
        if text.is_some() {
            self.revision += 1;
        }
        self.events.push_back(Event {
            event_id: self.last_event,
            kind: kind.into(),
            segment_id: text.as_ref().map(|_| 0),
            revision: text.as_ref().map(|_| self.revision),
            text,
            is_final: final_text,
        });
        while self.events.len() > self.limits.events {
            self.events.pop_front();
        }
    }
    pub fn partial(&mut self, text: String) -> Result<(), &'static str> {
        if text.len() > 4096 {
            return Err("transcript_limit");
        }
        if self.status.terminal() {
            return Err("late_callback");
        }
        self.emit("transcript_partial", Some(text), false);
        Ok(())
    }
    pub fn complete(&mut self, text: String) -> Result<(), &'static str> {
        if text.len() > 4096 {
            return Err("transcript_limit");
        }
        if self.status != Status::Draining {
            return Err("late_callback");
        }
        self.emit("transcript_final", Some(text), true);
        self.status = Status::Completed;
        self.emit("completed", None, false);
        Ok(())
    }
    pub fn cancel(&mut self) -> Result<(), &'static str> {
        if self.status == Status::Cancelled {
            return Ok(());
        }
        if self.status.terminal() {
            return Err("terminal_conflict");
        }
        self.status = Status::Cancelled;
        self.emit("cancelled", None, false);
        Ok(())
    }
    pub fn fail(&mut self) {
        if !self.status.terminal() {
            self.status = Status::Failed;
            self.emit("failed", None, false);
        }
    }
    pub fn cursor(&self) -> u32 {
        self.last_event
    }
    pub fn read(&self, after: u32, limit: usize) -> Result<Vec<Event>, &'static str> {
        if after > self.last_event {
            return Err("cursor_ahead");
        }
        if self.events.front().is_some_and(|e| after < e.event_id - 1) {
            return Err("cursor_expired");
        }
        Ok(self
            .events
            .iter()
            .filter(|e| e.event_id > after)
            .take(limit)
            .cloned()
            .collect())
    }
}
