# tiny-event-emitter

> **Zero-dependency Node.js-style EventEmitter.** Pub/sub, async listeners, once, wildcard patterns, thread-safe variant. Pure Python stdlib. One file. MIT.

```
pip install tiny-event-emitter
```
or just copy `tiny_event_emitter.py` into your project.

---

## Features

- ✅ **Node.js EventEmitter API** — `on`, `once`, `off`, `emit`, `listeners`, `listenerCount`
- ✅ **Async listeners** — `emit_async()` runs all listeners concurrently with `asyncio.gather`
- ✅ **Wildcard patterns** — `"*"` catches all events; `"user:*"` catches `user:login`, `user:logout`, etc.
- ✅ **Error handling** — `on_error()` handler catches exceptions from listeners
- ✅ **Chaining** — all registration methods return `self`
- ✅ **Thread-safe variant** — `ThreadEventEmitter` for multi-threaded use
- ✅ **Zero dependencies** — stdlib only

---

## Quick Start

```python
from tiny_event_emitter import EventEmitter

emitter = EventEmitter()

def on_click(data):
    print(f"Clicked: {data}")

# Register
emitter.on("click", on_click)

# Also works as a decorator
@emitter.on("data")
def handle_data(msg):
    print(msg)

# One-time listener
@emitter.once("ready")
def once_handler():
    print("Ready!")

# Emit
emitter.emit("click", {"x": 100, "y": 200})  # True (listeners fired)
emitter.emit("silent")                         # False (no listeners)

# Query
emitter.listener_count("click")  # 2
emitter.listeners("click")       # [on_click, handle_data]
emitter.event_names()            # ["click", "data", "ready"]

# Remove
emitter.off("click", on_click)
```

---

## Async Listeners

```python
import asyncio
from tiny_event_emitter import EventEmitter

emitter = EventEmitter()

@emitter.on("fetch")
async def fetch_data(url):
    await asyncio.sleep(0.1)
    return {"url": url, "status": 200}

async def main():
    results = await emitter.emit_async("fetch", "https://example.com")
    print(results)

asyncio.run(main())
```

---

## Wildcard Patterns

```python
emitter = EventEmitter()

emitter.on("user:*", lambda data: print(f"User event: {data}"))
emitter.on("error.*", lambda data: print(f"Error: {data}"))

emitter.emit("user:login", {"user": "alice"})  # catches "user:*"
emitter.emit("user:logout", {"user": "alice"}) # catches "user:*"
emitter.emit("error:db", {"msg": "timeout"})   # catches "error.*"
```

---

## Error Handling

```python
emitter = EventEmitter()

@emitter.on_error
def handle_error(exc, event, *args, **kwargs):
    print(f"Error in {event}: {exc}")
    # Log to Sentry, rollbar, etc.

@emitter.on("risky")
def risky_handler():
    raise RuntimeError("boom")

emitter.emit("risky")  # handle_error called; no exception thrown
```

---

## Thread Safety

```python
from tiny_event_emitter import ThreadEventEmitter

# Use ThreadEventEmitter in multi-threaded contexts
emitter = ThreadEventEmitter()
emitter.on("tick", lambda: None)

import threading
t1 = threading.Thread(target=lambda: emitter.emit("tick"))
t2 = threading.Thread(target=lambda: emitter.emit("tick"))
t1.start(); t2.start()
t1.join(); t2.join()
```

---

## Benchmark

```python
# 100k emit cycles on M3 Pro
# ~2.8 µs per emit (3 listeners, sync)
# ~0.4 ms per emit_async (10 concurrent async listeners)
```

---

## Install

```bash
pip install tiny-event-emitter
```

Or copy the single file:
```bash
curl -O https://raw.githubusercontent.com/hussain-alsaibai/tiny-event-emitter/main/tiny_event_emitter.py
```

---

## Two implementations

This package ships two independent, zero-dependency implementations. Both are
tested on Python 3.8-3.12 and both are installed by `pip install tiny-event-emitter`.

| Module | Style | Use it when |
|---|---|---|
| `tiny_event_emitter` | Node.js `EventEmitter` API | You want `on`/`once`/`off`/`emit` that returns `self`, `prepend`, `on_error`, `set_max_listeners`, or a thread-safe variant. This is the default and the one the README above describes. |
| `event_emitter` | Async-first, event-object API | You want `on_async`, explicit `on_wildcard`/`off_wildcard`, `emit_loop`, `clear`, an `Event` object, or Server-Sent Events via `sse_format()`. |

They do not share a base class and are not interchangeable - the method
signatures differ (`emit()` returns `bool` in one and `list` in the other, and
`off()` takes a function in one and a listener id in the other). Import the one
whose API matches what you are writing.

```python
from tiny_event_emitter import EventEmitter   # Node-style
from event_emitter import EventEmitter, sse_format  # async-first
```

---

## License

MIT © 2026 Hussain Al-Saibai
