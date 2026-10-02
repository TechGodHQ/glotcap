# GLOTCAP: bounded domain + synthetic session runtime

Local Rust 2024 core intended for independent **TechGodHQ/glotcap**. Existing
**TechGodHQ/glotcap-web** is separate and unchanged. This is a tested library
slice, not a runnable server/CLI or speech recognizer.

## Implemented

- `crates/glotcap`: pure PCM, sequence/retry, bounded replay/event retention,
  transcript revision and session transition rules.
- `crates/glotcap-server`: bounded in-memory workers, one unary `tool` dispatch,
  native replay/live observers, cancellation, synthetic failure and shutdown.
- `api/operations.yaml`, `hydra.yaml`, `generated/`: actual Hydra-generated
  planned projections. They are **not compiled into or connected to this core**.
  No build script silently regenerates them.

Input is explicitly `pcm_s16le_16000_mono`. Audio chunks are nonempty aligned
PCM16, at most 3200 decoded bytes, with base64 strings checked against 4268
characters before decoding. Sequence starts at zero. An exact retained retry
returns the original receipt even after termination; conflicting retries, expired
retries and gaps fail explicitly. Receipt means in-memory acceptance, not durable
storage or recognized speech. A rejected append never advances sequence.

Defaults: 16 queued chunks plus one in-flight chunk per session, 64 retained
sessions (including terminal sessions), 64 replay receipts, 128 events, 16 observers
per session, and `32000 * 60` total decoded bytes per session. Transcript payloads
are capped at 4096 UTF-8 bytes. A full queue returns `backpressure`. Session slots
are deliberately never evicted/reused: a full registry returns `session_limit`
until a new host is constructed. No unbounded history or work queue exists.

`finish_session` promptly returns `draining`, closes ingress, and allows every
accepted chunk to be processed. The synthetic provider resource is dropped before
final transcript + exactly one completed event are published atomically. Every
transcript uses segment 0 with increasing revisions; final is immutable.

`cancel_session` promptly acknowledges `cancelling` (or already `cancelled`).
Terminal cancellation is asynchronous: observe `cancelled` via polling/observer
before assuming teardown. Cancellation bypasses a blocked provider future,
discards queued audio, drops the provider guard, and only then emits one terminal
cancelled event with no fabricated final. Failures similarly release resources
before one failed event. Terminal states reject new callbacks. Exact retained
audio retries remain receipts, not new work.

Observer attachment validates the replay cursor and subscribes under the same
session lock. Each observer reads retained events by cursor; coalesced watch
notifications do not lose event ordering. A slow observer gets `observer_lag`
and EOF; an expired reconnect cursor gets `cursor_expired`. Dropping an observer
does not cancel speech work. `read_events` returns a bounded page, status and
`next_cursor`. Provider work never runs under global/session locks.

Call and await `AppState::shutdown()` for verified task joins. Concurrent callers
serialize each worker join; dropping a pending shutdown future leaves the stored
join handle available for a retry. Every successful shutdown return follows all
worker joins and provider release. Dropping the final host requests cancellation
as a fallback; it cannot synchronously await teardown.
The synthetic provider has normal, deliberately blocked, and failing modes;
`calls()` and `live()` are deterministic test instrumentation. Its output always
starts with **SYNTHETIC** and counts samples only. It does **not** infer words from
audio, open a microphone, invoke a model, or bill a provider.

## Verify

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --all-targets
```

Hydra source baseline: `303ba7af54b2c54752c59c1e3a60828290f79e75`.
With that checkout's built `hydra-codegen` binary, from this root:

```sh
/path/to/hydra/target/debug/hydra-codegen write
/path/to/hydra/target/debug/hydra-codegen check
```

Do not cargo-format the standalone generated projections: Hydra owns their exact
bytes. No local Hydra checkout is needed to build the two core crates.

## Explicitly missing

No HTTP listener/router/SSE binding, MCP HTTP/stdio host, generated CLI integration,
shared-service CLI client, generated TypeScript integration or cross-surface
parity verification. The interrupted empty CLI executable was removed rather than
claimed as working. Native `subscribe` is not yet a subscription-returning branch
of a unified transport dispatch; that is part of future surface integration.

No real provider adapter/trait, external cancellation/flush proof, provider callback
bridge, auth/ownership/CORS/body-size transport policy, codecs, persistent storage,
terminal retention TTL/eviction, UI or migration. No real network benchmark or
continuous/duplex audio transport. A future provider requires explicit drain and
teardown semantics; this synthetic sample counter is not such an implementation.

This implementation remains local and unpublished: no push, release, billing or
tickets were performed. The separate remote **TechGodHQ/glotcap** already exists,
initialized with an MIT license; this local implementation has not been published
to it.
See [DOCTRINE.md](DOCTRINE.md) for scope and continuation constraints.
