# Changelog

## [1.0.1] - 2026-11-19

### Fixed — `event_emitter.py` had no tests, and six real defects

Every test in this repo covered `tiny_event_emitter`. The second module
shipped in 1.0.0 with zero coverage, and the defects below are what that
bought. All 31 tests added here pass; 16 of them fail against 1.0.0.

- **`emit_async()` never ran sync listeners.** `on(fn)` + `await
  emit_async(...)` — the exact pattern in this module's own docstring —
  silently did nothing. The result list was simply short, with no error.
  Both kinds now dispatch, in registration order.
- **`on("db:*", fn)` did not match `db:open`.** Pattern registrations were
  stored in the exact-name bucket and looked up literally, so only
  `on_wildcard()` ever matched anything. Every bucket is now keyed-checked
  against the pattern matcher.
- **`off(event, listener_id)` always returned `False`.** It filtered the id
  out and *then* searched the filtered list for it. Unknown ids also raised
  `KeyError` on the defaultdict instead of returning `False`.
- **`emit()` dropped events destined for async listeners.** A queue-backed
  consumer (the `on_async()` streams) received nothing from a synchronous
  producer. Async listeners are now scheduled on the running loop; with no
  loop available they are skipped **with a `RuntimeWarning`** rather than
  silently.
- **A sync listener returning a coroutine leaked it unawaited.** It was
  returned as a coroutine object in the results list. Every listener now
  goes through `_run_async`, which awaits when needed and passes plain
  values through.
- **`on_async()` and `once()` could not be used as documented.** Both were
  `async def` returning a generator, so `async for x in on_async(...)` raised
  `TypeError: 'async for' requires an object with __aiter__`. And because
  they subscribed lazily, everything emitted before the first iteration was
  lost. Both are now regular functions that subscribe eagerly.

### Fixed — Python 3.8

- `asyncio.Queue()` binds to the current loop at construction on 3.8, so a
  stream created outside a coroutine raised
  `RuntimeError: There is no current event loop`. The queue is replaced by a
  list plus a lazily-created future. Verified on 3.8 and 3.11, not assumed.

### Added

- `test_event_emitter.py` — 31 tests covering dispatch, once semantics,
  `off()`, patterns, introspection, streams, SSE framing and thread safety.
- `examples/async_streams.py` — executable documentation for the async-first
  API. Every claim is an assertion; the script exits non-zero if one breaks.
- README: a full `event_emitter` section with the stream and pattern
  contracts spelled out.

### Changed

- Package description now names both APIs, so the second module is
  discoverable from the registry listing rather than only from the source.
