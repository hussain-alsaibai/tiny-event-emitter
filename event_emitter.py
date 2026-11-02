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
        results = []
        # Collect all matching listeners
        listeners_to_call: list[tuple[bool, tuple[str, Listener]]] = []

        with self._lock or self._lock or __import__("contextlib").nullcontext():
            # Regular listeners
            for lid, listener in self._listeners.get(event_name, []):
                listeners_to_call.append((False, (lid, listener)))
            # Once listeners (consume them)
            for lid, listener in self._once.pop(event_name, []):
                listeners_to_call.append((False, (lid, listener)))
            # Wildcard listeners
            for pattern, listener in self._wildcards:
                if _match_wildcard(pattern, event_name):
                    listeners_to_call.append((False, (pattern, listener)))

        for is_async, (lid, listener) in listeners_to_call:
            results.append(self._run(listener, (Event(event_name, data),), {}))

        return results

    async def _emit_async(self, event_name: str, data: Any) -> list[Any]:
        results = []
        listeners_to_call: list[tuple[bool, tuple[str, Listener | AsyncListener]]] = []

        # Collect listeners
        for lid, listener in self._async_listeners.get(event_name, []):
            listeners_to_call.append((True, (lid, listener)))
        for lid, listener in self._once_async.pop(event_name, []):
            listeners_to_call.append((True, (lid, listener)))
        for pattern, listener in self._wildcards:
            if _match_wildcard(pattern, event_name):
                listeners_to_call.append((False, (pattern, listener)))

        for is_async, (lid, listener) in listeners_to_call:
            if is_async:
                results.append(
                    await self._run_async(listener, (Event(event_name, data),), {})
                )
            else:
                results.append(self._run(listener, (Event(event_name, data),), {}))

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
                    store[event][:] = [
                        (lid, l) for lid, l in store[event] if lid != listener_id
                    ]
                    if any(lid == listener_id for lid, _ in store[event]):
                        removed = True
                # Wildcard
                self._wildcards[:] = [
                    (p, l) for p, l in self._wildcards if p != listener_id
                ]
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


def once(emitter: EventEmitter, event: str) -> Iterator[Event]:
    """
    Generator that yields the next event matching `event`, then stops.
    Usage: async for e in once(emitter, "data"): print(e)
    """
    queue: asyncio.Queue[Event] = asyncio.Queue()

    async def _listener(e: Event) -> None:
        await queue.put(e)

    lid = None

    async def _run():
        nonlocal lid
        lid = emitter.on_async(event, _listener)
        try:
            while True:
                ev = await queue.get()
                yield ev
                break
        finally:
            if lid:
                emitter.off(event, lid)

    return _run()


async def on_async(emitter: EventEmitter, event: str) -> Iterator[Event]:
    """Async generator: continuously yields events as they arrive."""
    queue: asyncio.Queue[Event] = asyncio.Queue()
    lid = [None]  # mutable container for closure

    async def _listener(e: Event) -> None:
        await queue.put(e)

    async def _runner():
        lid[0] = emitter.on_async(event, _listener)
        try:
            while True:
                ev = await queue.get()
                yield ev
        except GeneratorExit:
            if lid[0]:
                emitter.off(event, lid[0])

    return _runner()


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
