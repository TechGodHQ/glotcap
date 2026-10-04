# GLOTCAP core doctrine

- Keep this independent Rust core separate from glotcap-web. Do not migrate UI,
  Convex data or legacy behavior without separate approval.
- Call synthetic output SYNTHETIC everywhere. Sample counts are not transcripts
  of speech. No real provider capability or cancellation claim follows from these
  tests.
- Keep domain rules free of runtime/provider/transport I/O. Bound every retained
  queue/log/receipt/session/observer and transcript payload. Reject overload rather
  than silently dropping accepted audio.
- Finish is a drain request, not completion. Release provider resources before
  final transcript and the single completed event. Cancellation/failure must
  never fabricate a final transcript. Reject callbacks after terminal state.
- Run provider work outside locks. Preserve the same pinned in-flight future
  across ingress/finish notifications; only cancellation should drop it. Do not
  restart work or count accepted audio twice because of a control wake.
- Observer disconnect is not domain cancellation. Lag and cursor expiry must be
  explicit. Replay+live attachment must be atomic.
- Keep the hard retained-session cap until a separately tested eviction policy
  exists. Do not hide unlimited terminal-session retention behind a concurrency
  limit. Graceful shutdown must join workers; last-host drop is fallback only.
- Future transport integration must use the explicit Hydra operation definitions,
  committed generated projections and one shared dispatch. Include native
  subscription dispatch before claiming cross-surface completion. CLI commands
  must address the same long-lived host, not create independent ephemeral stores.
- Use Hydra write/check explicitly; no build-time silent repair. Keep SSE binding
  metadata-driven. Buffered chunks are not continuous request streaming.
- Test failure first for behavior changes; gate fmt, clippy with warnings denied,
  build and tests. Compile and exercise surfaces before calling them implemented.
- Do not expand into codec/provider registries, orchestration, persistence, UI,
  releases or external resources in this slice.
