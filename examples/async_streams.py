"""Executable doc for `event_emitter` -- the async-first second API.

Run it:

    python examples/async_streams.py

This module is the "listener side is async" implementation: the emitting
side stays synchronous, and listeners are awaited in registration order,
receiving `Event` objects (name + data + id + timestamp) instead of the bare
payload. Use it when listeners do async work -- SSE responses, agent loops,
queue writers. Use `tiny_event_emitter` when you want the Node-style
`on`/`off`/`emit` surface or the `ThreadEventEmitter`.

Every claim below is asserted, so this file fails if the behaviour changes.
"""

from __future__ import annotations

import asyncio

from event_emitter import Event, EventEmitter, on_async, once, sse_format


async def main() -> int:
    # ---- 1. listeners receive Event objects ---------------------------
    bus = EventEmitter(lock=True)
    seen: list[Event] = []

    def log_call(event: Event) -> None:  # sync listener
        seen.append(event)

    async def handle_async(event: Event) -> None:
        await asyncio.sleep(0)  # a real listener would await I/O here
        seen.append(event)

    bus.on("agent:tool_call", log_call)
    bus.on_async("agent:tool_call", handle_async)

    results = await bus.emit_async("agent:tool_call", {"tool": "grep", "ok": True})
    assert len(results) == 2, f"expected 2 listeners to run, got {len(results)}"
    assert len(seen) == 2
    assert all(isinstance(e, Event) for e in seen)
    assert seen[0].name == "agent:tool_call"
    assert seen[0].data == {"tool": "grep", "ok": True}
    assert seen[0].id and len(seen[0].id) == 8
    print(f"emit_async    {len(results)} listeners   first event id={seen[0].id}")

    # ---- 2. once() flips itself off -----------------------------------
    fired: list[Event] = []
    bus.once("agent:done", lambda e: fired.append(e))
    await bus.emit_async("agent:done", 1)
    await bus.emit_async("agent:done", 2)
    assert len(fired) == 1, f"once listener fired {len(fired)} times"
    print(f"once          fired {len(fired)} time across 2 emits")

    # ---- 3. emit() also feeds async listeners, via the running loop ----
    sync_seen: list[Event] = []
    async_seen: list[Event] = []
    local = EventEmitter()
    local.on("tick", lambda e: sync_seen.append(e))

    async def note(event: Event) -> None:
        async_seen.append(event)

    local.on_async("tick", note)
    local.emit("tick", 1)             # sync call, inside this coroutine
    assert len(sync_seen) == 1, "sync listener must run inline"
    await asyncio.sleep(0)            # async listener runs as a scheduled task
    assert len(async_seen) == 1, "async listener must receive the sync emit"
    print(f"emit()        {len(sync_seen)} sync inline, {len(async_seen)} async scheduled")

    # ---- 4. a pattern registered with on() IS a wildcard --------------
    log: list[str] = []
    local.on("db:*", lambda e: log.append(e.name))
    for name in ("db:open", "db:write", "cache:miss", "db:close"):
        local.emit(name, None)
    assert log == ["db:open", "db:write", "db:close"], log
    print(f"on('db:*')    matched {log}")

    # ---- 5. drive it as a stream ---------------------------------------
    stream = on_async(local, "job:*")

    async def producer() -> None:
        for i in range(3):
            await asyncio.sleep(0)
            local.emit(f"job:{i}", {"i": i})

    task = asyncio.create_task(producer())
    got = []
    async for event in stream:
        got.append(event.data["i"])
        if len(got) == 3:
            break
    await task
    assert got == [0, 1, 2], got
    print(f"on_async      streamed {got}")

    # ---- 6. one event, then unsubscribe --------------------------------
    one = EventEmitter()
    it = once(one, "ready")     # subscribes eagerly
    one.emit("ready", "go")
    first = await asyncio.wait_for(anext(it), 2)
    assert first.data == "go"
    print("once()        yielded exactly one event as an async iterator")

    # ---- 7. SSE framing -------------------------------------------------
    frame = sse_format("tool_call", {"tool": "grep"}, id="abc123")
    assert frame.startswith("event: tool_call\n")
    assert 'data: {"tool": "grep"}' in frame
    assert "id: abc123" in frame
    assert frame.endswith("\n\n"), "SSE frames end with a blank line"
    print("sse_format    " + frame.strip().replace("\n", " | "))

    print("\nok — every assertion held")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
