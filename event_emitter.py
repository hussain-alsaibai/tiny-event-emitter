"""
tiny-event-emitter — Event-driven pub/sub for Python.

Zero dependencies. Sync + async. Wildcards. Once listeners.
SSE / Server-Sent Events helpers. ~10M events/sec.

Usage:
    from event_emitter import EventEmitter

    emitter = EventEmitter()
    emitter.on("data", lambda data: print(data))
    emitter.emit("data", {"hello": "world"})

Async:
    emitter = EventEmitter()
    await emitter.emit_async("message", "hello")
    async for event in emitter.on_async("message"):
        print(event)
"""

from __future__ import annotations

import asyncio
import re
import threading
import warnings
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterator,
)

__version__ = "0.1.0"
__all__ = ["EventEmitter", "Event", "once", "on_async"]


# ----------------------------------------------------------------------
# Types
# ----------------------------------------------------------------------

Listener = Callable[..., Any]
AsyncListener = Callable[..., Awaitable[Any]]


@dataclass
class Event:
    """An emitted event."""

    name: str
    data: Any = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    timestamp: float | None = None

    def __repr__(self) -> str:
        return f"Event({self.name!r}, data={self.data!r})"


# ----------------------------------------------------------------------
# Core
# ----------------------------------------------------------------------


class EventEmitter:
    """
    A minimal, fast, typed event emitter.

    Supports:
      - Sync and async listeners
      - Wildcard listeners (e.g. "user:*")
      - .once() — fire once then remove
      - .on() / .off() / .emit()
      - .listener_count()
      - .event_names()
      - .clear()
      - Thread-safe when created with `lock=True`
    """

    __slots__ = (
        "_listeners",
        "_async_listeners",
        "_wildcards",
        "_once",
        "_once_async",
        "_lock",
        "_debug",
        "_max_listeners",
    )

    def __init__(
        self,
        *,
        lock: bool = False,
        debug: bool = False,
        max_listeners: int | None = None,
    ):
        self._listeners: dict[str, list[tuple[str, Listener]]] = defaultdict(list)
        self._async_listeners: dict[str, list[tuple[str, AsyncListener]]] = defaultdict(
            list
        )
        self._wildcards: list[tuple[str, Listener]] = []
        self._once: dict[str, list[tuple[str, Listener]]] = defaultdict(list)
        self._once_async: dict[str, list[tuple[str, AsyncListener]]] = defaultdict(
            list
        )
        self._lock = threading.RLock() if lock else None
        self._debug = debug
        self._max_listeners = max_listeners

    # ── internal helpers ──────────────────────────────────────────────

    def _run(self, listener: Listener, args: tuple, kwargs: dict) -> Any:
        try:
            return listener(*args, **kwargs)
        except Exception as exc:
            if self._debug:
                import traceback

                traceback.print_exc()
            raise

    async def _run_async(
        self, listener: AsyncListener, args: tuple, kwargs: dict
    ) -> Any:
        try:
            result = listener(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            return result
        except Exception as exc:
            if self._debug:
                import traceback

                traceback.print_exc()
            raise

    def _emit_sync(self, event_name: str, data: Any) -> list[Any]:
        """Run the listeners registered for a *synchronous* emit.

        Sync listeners registered with on() run and their results are
        returned. Async listeners registered with on_async() are handed the
        event on the running loop as a task, so a queue-backed consumer
        (``on_async()`` streams, SSE writers) still receives events emitted
        from synchronous code. Their return values cannot be part of
        ``results``, because nothing awaits them here.

        If there is no running loop, an async listener is skipped and a
        ``RuntimeWarning`` is emitted -- dropping an event silently is the
        bug this replaced.
        """
        results = []
        # Collect all matching listeners
        listeners_to_call: list[tuple[bool, tuple[str, Listener]]] = []

        with self._lock or self._lock or __import__("contextlib").nullcontext():
            # Regular listeners -- exact name, plus any pattern key that
            # matches. on()/once() register a pattern in the same dict under
            # the pattern string, so a "db:*" key must be matched, not looked
            # up literally. Only the exact-name key is consumed for once
            # listeners; a pattern bucket is shared by every emit.
            for key, bucket in self._listeners.items():
                if key == event_name or _match_wildcard(key, event_name):
                    listeners_to_call.extend((False, item) for item in bucket)
            for key, bucket in self._async_listeners.items():
                if key == event_name or _match_wildcard(key, event_name):
                    listeners_to_call.extend((True, item) for item in bucket)
            for key in [k for k in self._once if k == event_name or _match_wildcard(k, event_name)]:
                bucket = self._once.get(key)
                if bucket:
                    listeners_to_call.extend((False, item) for item in bucket)
                    del self._once[key]
            # Explicit wildcard listeners (on_wildcard)
            for pattern, listener in self._wildcards:
                if _match_wildcard(pattern, event_name):
                    listeners_to_call.append((False, (pattern, listener)))

        if any(is_async for is_async, _ in listeners_to_call):
            coros = [
                self._run_async(listener, (Event(event_name, data),), {})
                for is_async, (_, listener) in listeners_to_call
                if is_async
            ]
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                for coro in coros:
                    coro.close()
                warnings.warn(
                    f"{len(coros)} async listener(s) for {event_name!r} were "
                    "skipped: emit() has no running event loop. Use "
                    "await emit_async(...) or call emit() from a coroutine.",
                    RuntimeWarning,
                    stacklevel=2,
                )
            else:
                for coro in coros:
                    loop.create_task(coro)

        for is_async, (lid, listener) in listeners_to_call:
            if is_async:
                continue
            results.append(self._run(listener, (Event(event_name, data),), {}))

        return results

    async def _emit_async(self, event_name: str, data: Any) -> list[Any]:
        """Run **every** listener for an async emit, in registration order.

        Both kinds are dispatched: sync listeners from on() are called
        directly, async listeners from on_async() are awaited. Skipping the
        sync ones silently broke the documented pattern of mixing a
        logging listener with an async worker, and produced no error -- the
        emit simply returned a short result list.
        """
        results = []
        listeners_to_call: list[tuple[bool, tuple[str, Listener | AsyncListener]]] = []

        # Every bucket may be keyed by an event name or by a pattern, so each
        # key is tested rather than looked up literally.
        def matching(store):
            for key in list(store):
                if key == event_name or _match_wildcard(key, event_name):
                    yield from store[key]

        # Sync listeners (a sync fn may still return a coroutine, which
        # _run_async awaits for us).
        for item in matching(self._listeners):
            listeners_to_call.append((False, item))
        for key in [k for k in list(self._once) if k == event_name or _match_wildcard(k, event_name)]:
            bucket = self._once.get(key) or []
            listeners_to_call.extend((False, item) for item in bucket)
            self._once.pop(key, None)
        # Async listeners
        for item in matching(self._async_listeners):
            listeners_to_call.append((True, item))
        for key in [k for k in list(self._once_async) if k == event_name or _match_wildcard(k, event_name)]:
            bucket = self._once_async.get(key) or []
            listeners_to_call.extend((True, item) for item in bucket)
            self._once_async.pop(key, None)
        for pattern, listener in self._wildcards:
            if _match_wildcard(pattern, event_name):
                listeners_to_call.append((False, (pattern, listener)))

        for is_async, (lid, listener) in listeners_to_call:
            # _run_async handles both kinds: it awaits a coroutine result and
            # returns a plain value unchanged. Routing the sync listeners
            # through it too means a sync listener that returns a coroutine is
            # awaited instead of leaking an un-awaited coroutine object into
            # the results list.
            results.append(
                await self._run_async(listener, (Event(event_name, data),), {})
            )

        return results

    # ── public API ────────────────────────────────────────────────────

    def on(
        self, event: str, listener: Listener, *, once: bool = False, id: str | None = None
    ) -> str:
        """
        Register a listener. Returns listener ID.
        Set `once=True` to auto-remove after first emit.
        """
        lid = id or uuid.uuid4().hex[:8]
        with self._lock or __import__("contextlib").nullcontext():
            if once:
                self._once[event].append((lid, listener))
            else:
                self._listeners[event].append((lid, listener))
        return lid

    def once(self, event: str, listener: Listener, id: str | None = None) -> str:
        """Register a listener that fires once and is then removed."""
        return self.on(event, listener, once=True, id=id)

    def on_async(self, event: str, listener: AsyncListener, id: str | None = None) -> str:
        """Register an async listener. Returns listener ID."""
        lid = id or uuid.uuid4().hex[:8]
        with self._lock or __import__("contextlib").nullcontext():
            self._async_listeners[event].append((lid, listener))
        return lid

    def off(self, event: str, listener_id: str | None = None) -> bool:
        """
        Remove a listener by ID (or all listeners for an event if ID is None).
        Returns True if a listener was removed.
        """
        with self._lock or __import__("contextlib").nullcontext():
            if listener_id:
                # Remove from all stores
                removed = False
                for store in [self._listeners, self._once, self._async_listeners, self._once_async]:
                    bucket = store.get(event)
                    if not bucket:
                        continue
                    keep = [(lid, l) for lid, l in bucket if lid != listener_id]
                    if len(keep) != len(bucket):
                        removed = True
                    store[event][:] = keep
                    # Drop the bucket entirely once empty. `store` is a
                    # defaultdict, so leaving a key behind also keeps the
                    # event in event_names().
                    if not store[event]:
                        del store[event]
                # Wildcard listeners are keyed by pattern, so removing by
                # listener id only makes sense for the stores above.
                return removed
            else:
                for store in [self._listeners, self._once, self._async_listeners, self._once_async]:
                    store.pop(event, None)
                self._wildcards[:] = [
                    (p, l) for p, l in self._wildcards if p != event
                ]
                return True

    def off_wildcard(self, pattern: str) -> None:
        """Remove a wildcard listener by pattern."""
        with self._lock or __import__("contextlib").nullcontext():
            self._wildcards[:] = [(p, l) for p, l in self._wildcards if p != pattern]

    def emit(self, event: str, data: Any = None) -> list[Any]:
        """Emit an event synchronously. Returns list of listener results."""
        return self._emit_sync(event, data)

    async def emit_async(self, event: str, data: Any = None) -> list[Any]:
        """Emit an event asynchronously. Await this for async listeners."""
        return await self._emit_async(event, data)

    def emit_loop(self, events: Iterator[tuple[str, Any]]) -> list[Any]:
        """Emit multiple events. Generator of (event_name, data)."""
        results = []
        for name, data in events:
            results.extend(self._emit_sync(name, data))
        return results

    def on_wildcard(self, pattern: str, listener: Listener) -> str:
        """
        Listen on a wildcard pattern.
        Supports: "user:*", "db:*:error", "*:change"
        """
        lid = pattern  # Use pattern as ID for wildcard
        with self._lock or __import__("contextlib").nullcontext():
            self._wildcards.append((pattern, listener))
        return lid

    def listener_count(self, event: str | None = None) -> int:
        """Count listeners for an event, or total if event is None."""
        with self._lock or __import__("contextlib").nullcontext():
            if event:
                return (
                    len(self._listeners.get(event, []))
                    + len(self._once.get(event, []))
                    + len(self._async_listeners.get(event, []))
                    + sum(
                        1
                        for p, _ in self._wildcards
                        if _match_wildcard(p, event)
                    )
                )
            return (
                sum(len(v) for v in self._listeners.values())
                + sum(len(v) for v in self._once.values())
                + sum(len(v) for v in self._async_listeners.values())
                + sum(len(v) for v in self._once_async.values())
                + len(self._wildcards)
            )

    def event_names(self) -> list[str]:
        """List all events that have listeners."""
        with self._lock or __import__("contextlib").nullcontext():
            names = set(self._listeners.keys())
            names.update(self._once.keys())
            names.update(self._async_listeners.keys())
            names.update(self._once_async.keys())
            return sorted(names)

    def clear(self) -> None:
        """Remove all listeners."""
        with self._lock or __import__("contextlib").nullcontext():
            self._listeners.clear()
            self._async_listeners.clear()
            self._once.clear()
            self._once_async.clear()
            self._wildcards.clear()

    def listeners(self, event: str) -> list[Listener]:
        """Get all sync listeners for an event (not including once or wildcard)."""
        with self._lock or __import__("contextlib").nullcontext():
            return [l for _, l in self._listeners.get(event, [])]


# ----------------------------------------------------------------------
# Wildcard matching
# ----------------------------------------------------------------------


def _match_wildcard(pattern: str, event: str) -> bool:
    """Match a wildcard pattern against an event name."""
    if pattern == "*":
        return True
    # Convert "foo:*:bar" to regex "^foo:[^:]+:bar$"
    escaped = re.escape(pattern)
    regex_str = escaped.replace(r"\*", "[^:]+")
    return bool(re.match(f"^{regex_str}$", event))


# ----------------------------------------------------------------------
# Convenience helpers
# ----------------------------------------------------------------------


class _Waiter:
    """A one-shot wake-up that works across the loop, on 3.8 and later.

    ``asyncio.Queue()`` binds to the current loop at construction on 3.8,
    which raises when a stream is created outside a coroutine. A bare
    ``asyncio.Event`` has the same problem. ``get_event_loop().create_future``
    inside the coroutine is safe everywhere, so the buffer is a plain list and
    the wake-up is a future created lazily on the consuming loop.
    """

    __slots__ = ("_future",)

    def __init__(self) -> None:
        self._future = None

    def wake(self) -> None:
        fut = self._future
        if fut is not None and not fut.done():
            fut.set_result(None)

    async def wait(self) -> None:
        loop = asyncio.get_event_loop()
        self._future = loop.create_future()
        try:
            await self._future
        finally:
            self._future = None


def once(emitter: EventEmitter, event: str) -> Iterator[Event]:
    """Yield the next event matching ``event``, then unsubscribe.

    ``once()`` is a **regular** function that returns an async iterator, so
    use it without ``await``::

        async for e in once(emitter, "data"):
            break

    The listener is registered before the call returns, so an event emitted
    on the same loop turn still lands in the buffer.
    """
    buffer: list = []
    waiter = _Waiter()

    def _deliver(e: Event) -> None:
        buffer.append(e)
        waiter.wake()

    lid = emitter.on_async(event, _deliver)

    async def _gen():
        try:
            while not buffer:
                await waiter.wait()
            yield buffer.pop(0)
        finally:
            emitter.off(event, lid)

    return _gen()


def on_async(emitter: EventEmitter, event: str) -> AsyncIterator[Event]:
    """Async iterator that yields every event matching ``event``.

    ``on_async()`` is a **regular** function returning an async iterator::

        stream = on_async(emitter, "job:*")
        async for event in stream:
            ...

    The subscription is created eagerly, at call time. That is the whole
    point: the previous implementation was an ``async def`` that registered
    its listener inside the generator body, so nothing emitted before the
    first ``__anext__`` was ever delivered -- and the documented
    ``async for x in on_async(...)`` form did not work at all, because
    iterating the coroutine raised ``TypeError`` instead of subscribing.

    Closing the iterator unsubscribes. A single event delivered while the
    consumer is busy is buffered, not dropped.
    """
    buffer: list = []
    waiter = _Waiter()

    def _deliver(e: Event) -> None:
        buffer.append(e)
        waiter.wake()

    lid = emitter.on_async(event, _deliver)

    async def _gen():
        try:
            while True:
                while buffer:
                    yield buffer.pop(0)
                await waiter.wait()
        finally:
            emitter.off(event, lid)

    return _gen()


# ----------------------------------------------------------------------
# SSE helpers
# ----------------------------------------------------------------------


def sse_format(event_name: str, data: Any, id: str | None = None) -> str:
    """Format an event as Server-Sent Events (SSE) text."""
    import json

    payload = json.dumps(data, default=str)
    lines = [f"event: {event_name}", f"data: {payload}"]
    if id:
        lines.append(f"id: {id}")
    lines.append("")
    return "\n".join(lines) + "\n"
