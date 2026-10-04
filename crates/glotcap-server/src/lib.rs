//! Bounded in-memory session runtime. Synthetic transcripts are NOT speech recognition.
use base64::{Engine, engine::general_purpose::STANDARD};
use glotcap::{Limits, Session, Status};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, watch},
    task::JoinHandle,
};

#[derive(Clone, Copy)]
pub struct Config {
    pub queue: usize,
    /// Includes retained terminal sessions; slots are not automatically reused.
    pub sessions: usize,
    pub events: usize,
    pub replay: usize,
    pub max_bytes: usize,
    pub observers: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            queue: 16,
            sessions: 64,
            events: 128,
            replay: 64,
            max_bytes: 32000 * 60,
            observers: 16,
        }
    }
}
#[derive(Default)]
pub struct SyntheticFactory {
    blocked: bool,
    failing: bool,
    calls: AtomicUsize,
    live: AtomicUsize,
}
impl SyntheticFactory {
    pub fn blocked() -> Self {
        Self {
            blocked: true,
            ..Self::default()
        }
    }
    pub fn failing() -> Self {
        Self {
            failing: true,
            ..Self::default()
        }
    }
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }
    async fn process(&self, _audio: Vec<u8>) -> Result<(), &'static str> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.blocked {
            std::future::pending::<()>().await;
        }
        if self.failing {
            return Err("synthetic_failure");
        }
        Ok(())
    }
}
struct ProviderGuard(Arc<SyntheticFactory>);
impl Drop for ProviderGuard {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Inner {
    domain: Session,
    queue: VecDeque<Vec<u8>>,
    cancel: bool,
}
struct Entry {
    inner: Mutex<Inner>,
    wake: Notify,
    changed: watch::Sender<u32>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    observers: AtomicUsize,
}
struct Registry {
    entries: HashMap<String, Arc<Entry>>,
    next: u64,
    closed: bool,
}
impl Drop for Registry {
    fn drop(&mut self) {
        // Last host drop requests cancellation even if callers omitted graceful shutdown.
        for entry in self.entries.values() {
            entry.inner.lock().expect("session lock poisoned").cancel = true;
            entry.wake.notify_one();
        }
    }
}
#[derive(Clone)]
pub struct AppState {
    registry: Arc<Mutex<Registry>>,
    config: Config,
    factory: Arc<SyntheticFactory>,
    workers: Arc<AtomicUsize>,
}
impl AppState {
    pub fn new(config: Config, factory: Arc<SyntheticFactory>) -> Self {
        assert!(
            config.queue > 0
                && config.sessions > 0
                && config.events >= 2
                && config.replay > 0
                && config.observers > 0
        );
        Self {
            registry: Arc::new(Mutex::new(Registry {
                entries: HashMap::new(),
                next: 0,
                closed: false,
            })),
            config,
            factory,
            workers: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn synthetic(config: Config) -> Self {
        Self::new(config, Arc::new(SyntheticFactory::default()))
    }
    pub fn workers(&self) -> usize {
        self.workers.load(Ordering::SeqCst)
    }
    fn entry(&self, id: &str) -> Result<Arc<Entry>, &'static str> {
        self.registry
            .lock()
            .expect("registry lock poisoned")
            .entries
            .get(id)
            .cloned()
            .ok_or("session_not_found")
    }
    pub fn subscribe(&self, id: &str, after: u32) -> Result<Observer, &'static str> {
        let entry = self.entry(id)?;
        let inner = entry.inner.lock().expect("session lock poisoned");
        inner.domain.read(after, 1)?;
        entry
            .observers
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < self.config.observers).then_some(n + 1)
            })
            .map_err(|_| "observer_limit")?;
        let changed = entry.changed.subscribe();
        drop(inner);
        Ok(Observer {
            entry,
            changed,
            after,
            id: id.into(),
            ended: false,
        })
    }
    pub async fn shutdown(&self) {
        let entries: Vec<_> = {
            let mut r = self.registry.lock().expect("registry lock poisoned");
            r.closed = true;
            r.entries.values().cloned().collect()
        };
        for e in &entries {
            e.inner.lock().expect("session lock poisoned").cancel = true;
            e.wake.notify_one();
        }
        for e in entries {
            // Serialize joiners without holding registry/session locks. Keep the handle
            // stored while awaiting: cancelling this future only releases the async
            // guard, so another caller can resume the same cancellation-safe join.
            let mut task = e.task.lock().await;
            if let Some(handle) = task.as_mut() {
                handle.await.expect("session worker panicked");
                task.take();
            }
        }
    }
}
pub struct Observer {
    entry: Arc<Entry>,
    changed: watch::Receiver<u32>,
    after: u32,
    id: String,
    ended: bool,
}
impl Drop for Observer {
    fn drop(&mut self) {
        self.entry.observers.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Observer {
    pub async fn next(&mut self) -> Option<Result<Value, &'static str>> {
        if self.ended {
            return None;
        }
        loop {
            self.changed.borrow_and_update();
            {
                let inner = self.entry.inner.lock().expect("session lock poisoned");
                match inner.domain.read(self.after, 1) {
                    Err(_) => {
                        self.ended = true;
                        return Some(Err("observer_lag"));
                    }
                    Ok(events) => {
                        if let Some(event) = events.into_iter().next() {
                            self.after = event.event_id;
                            return Some(Ok(envelope(&self.id, event)));
                        }
                    }
                }
                if inner.domain.status.terminal() {
                    self.ended = true;
                    return None;
                }
            }
            if self.changed.changed().await.is_err() {
                self.ended = true;
                return None;
            }
        }
    }
}
fn envelope(id: &str, event: glotcap::Event) -> Value {
    let mut value = serde_json::to_value(event).expect("event serializable");
    value["session_id"] = json!(id);
    value
}
fn publish(entry: &Entry, inner: &Inner) {
    entry.changed.send_replace(inner.domain.cursor());
}
async fn worker(entry: Arc<Entry>, factory: Arc<SyntheticFactory>, workers: Arc<AtomicUsize>) {
    factory.live.fetch_add(1, Ordering::SeqCst);
    let guard = ProviderGuard(factory.clone());
    let mut samples = 0usize;
    let outcome = loop {
        let action = {
            let mut inner = entry.inner.lock().expect("session lock poisoned");
            if inner.cancel {
                break Status::Cancelled;
            }
            match inner.queue.pop_front() {
                Some(audio) => Some(audio),
                None if inner.domain.status == Status::Draining => break Status::Completed,
                None => None,
            }
        };
        if let Some(audio) = action {
            let count = audio.len() / 2;
            let result = continue_or_process(&entry, &factory, audio).await;
            let mut inner = entry.inner.lock().expect("session lock poisoned");
            if inner.cancel {
                break Status::Cancelled;
            }
            if result.is_err() {
                break Status::Failed;
            }
            samples += count;
            inner
                .domain
                .partial(format!("SYNTHETIC samples={samples}"))
                .expect("active callback");
            publish(&entry, &inner);
        } else {
            entry.wake.notified().await;
        }
    };
    // Drop provider resources before publishing any terminal event.
    drop(guard);
    workers.fetch_sub(1, Ordering::SeqCst);
    let mut inner = entry.inner.lock().expect("session lock poisoned");
    inner.queue.clear();
    if inner.cancel {
        inner.domain.cancel().expect("active cancel");
    } else {
        match outcome {
            Status::Completed => {
                inner
                    .domain
                    .complete(format!("SYNTHETIC final samples={samples}"))
                    .expect("draining completion");
            }
            Status::Cancelled => {
                inner.domain.cancel().expect("active cancel");
            }
            _ => inner.domain.fail(),
        }
    }
    publish(&entry, &inner);
}
async fn continue_or_process(
    entry: &Entry,
    factory: &SyntheticFactory,
    audio: Vec<u8>,
) -> Result<(), &'static str> {
    // Control notifications may also represent ingress/finish; they must not discard accepted audio.
    let process = factory.process(audio);
    tokio::pin!(process);
    loop {
        if entry.inner.lock().expect("session lock poisoned").cancel {
            return Err("cancelled");
        }
        let result = tokio::select! {
            biased;
            _ = entry.wake.notified() => continue,
            result = &mut process => result,
        };
        return result;
    }
}
pub async fn tool(state: &AppState, operation: &str, args: Value) -> Result<Value, &'static str> {
    if operation == "start_session" {
        if args["format"] != "pcm_s16le_16000_mono" {
            return Err("invalid_format");
        }
        let mut registry = state.registry.lock().expect("registry lock poisoned");
        if registry.closed {
            return Err("shutdown");
        }
        if registry.entries.len() >= state.config.sessions {
            return Err("session_limit");
        }
        registry.next = registry.next.checked_add(1).ok_or("session_id_limit")?;
        let id = registry.next.to_string();
        let (changed, _) = watch::channel(0);
        let entry = Arc::new(Entry {
            inner: Mutex::new(Inner {
                domain: Session::new(Limits {
                    replay: state.config.replay,
                    max_bytes: state.config.max_bytes,
                    events: state.config.events,
                }),
                queue: VecDeque::new(),
                cancel: false,
            }),
            wake: Notify::new(),
            changed,
            task: AsyncMutex::new(None),
            observers: AtomicUsize::new(0),
        });
        state.workers.fetch_add(1, Ordering::SeqCst);
        let task = tokio::spawn(worker(
            entry.clone(),
            state.factory.clone(),
            state.workers.clone(),
        ));
        *entry.task.try_lock().expect("unpublished task lock") = Some(task);
        registry.entries.insert(id.clone(), entry);
        return Ok(json!({"session_id":id,"status":"open"}));
    }
    let id = args["session_id"].as_str().ok_or("invalid_session_id")?;
    let entry = state.entry(id)?;
    let mut inner = entry.inner.lock().expect("session lock poisoned");
    match operation {
        "append_audio" => {
            let encoded = args["data_base64"].as_str().ok_or("invalid_base64")?;
            if encoded.len() > 4268 {
                return Err("invalid_pcm");
            }
            let audio = STANDARD.decode(encoded).map_err(|_| "invalid_base64")?;
            let sequence = args["sequence"]
                .as_u64()
                .and_then(|s| u32::try_from(s).ok())
                .ok_or("invalid_sequence")?;
            if let Some(receipt) = inner.domain.replay(sequence, &audio)? {
                return Ok(json!(receipt));
            }
            if inner.cancel {
                return Err("ingress_closed");
            }
            if inner.queue.len() >= state.config.queue {
                return Err("backpressure");
            }
            let receipt = inner.domain.append(sequence, audio.clone())?;
            inner.queue.push_back(audio);
            entry.wake.notify_one();
            Ok(json!(receipt))
        }
        "finish_session" => {
            if inner.cancel {
                return Err("terminal_conflict");
            }
            inner.domain.finish()?;
            entry.wake.notify_one();
            Ok(json!({"status":inner.domain.status}))
        }
        "cancel_session" => {
            if inner.domain.status == Status::Cancelled {
                return Ok(json!({"status":"cancelled"}));
            }
            if inner.domain.status.terminal() {
                return Err("terminal_conflict");
            }
            inner.cancel = true;
            entry.wake.notify_one();
            Ok(json!({"status":"cancelling"}))
        }
        "read_events" => {
            let after = args["after"]
                .as_u64()
                .and_then(|s| u32::try_from(s).ok())
                .ok_or("invalid_cursor")?;
            let events = inner.domain.read(after, state.config.events)?;
            let next = events.last().map_or(after, |e| e.event_id);
            Ok(
                json!({"session_id":id,"status":inner.domain.status,"next_cursor":next,"events":events.into_iter().map(|e| envelope(id,e)).collect::<Vec<_>>()}),
            )
        }
        _ => Err("unknown_operation"),
    }
}
