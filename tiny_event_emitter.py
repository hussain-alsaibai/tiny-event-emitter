"""
tiny-event-emitter — Zero-dependency Node.js-style EventEmitter.

Publish/subscribe, async listeners, once, error handling, wildcard.
Pure Python stdlib. MIT License.

Inspired by Node.js EventEmitter. Compatible with Python 3.8+.
"""
from __future__ import annotations

import asyncio
import re
import threading
from collections import defaultdict
from typing import Any, Callable, List

# ----------------------------------------------------------------------
# Types
# ----------------------------------------------------------------------

ListenerFunc = Callable[..., Any]
WildcardEvent = str


class ListenerInfo:
    """Metadata for a registered listener."""
    __slots__ = ("func", "once", "prepend", "id")

    def __init__(self, func: ListenerFunc, once: bool = False, prepend: bool = False, id: int = 0):
        self.func = func
        self.once = once
        self.prepend = prepend
        self.id = id


# ----------------------------------------------------------------------
# EventEmitter
# ----------------------------------------------------------------------

class EventEmitter:
    """
    Minimal EventEmitter implementing the Node.js API surface.

    Supports:
      - sync and async listeners
      - once() listeners (auto-removed after first call)
      - prepend listeners (run before regular)
      - error event (special handling)
      - wildcard '*' and 'prefix*' patterns
      - thread-safe variant via ThreadEventEmitter
    """

    _LISTENER_ID = 0
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._events: dict[str, List[ListenerInfo]] = defaultdict(list)
        self._wildcards: dict[str, List[ListenerInfo]] = defaultdict(list)
        self._max_listeners: int | None = None
        self._error_handlers: List[Callable] = []

    # ------------------------------------------------------------------
    # Core registration
    # ------------------------------------------------------------------

    def on(self, event: str, func: ListenerFunc, prepend: bool = False) -> "EventEmitter":
        """Register a listener for `event`. Returns `func` for chaining."""
        info = ListenerInfo(func=func, prepend=prepend, id=self._next_id())
        self._add_listener(event, info)
        return self

    def once(self, event: str, func: ListenerFunc, prepend: bool = False) -> ListenerFunc:
        """Register a one-time listener (removed after first emit)."""
        info = ListenerInfo(func=func, once=True, prepend=prepend, id=self._next_id())
        self._add_listener(event, info)
        return func

    def off(self, event: str, func: ListenerFunc | None = None) -> "EventEmitter":
        """Remove a specific listener, or all listeners for event if func is None."""
        if func is None:
            self._events[event].clear()
        else:
            self._events[event] = [li for li in self._events[event] if li.func != func]
        self._rebuild_wildcard_cache()
        return self

    def remove_all_listeners(self, event: str | None = None) -> "EventEmitter":
        """Remove all listeners for `event`, or all events if None."""
        if event is None:
            self._events.clear()
            self._wildcards.clear()
        elif event in self._events:
            del self._events[event]
        self._rebuild_wildcard_cache()
        return self

    # ------------------------------------------------------------------
    # Emitting
    # ------------------------------------------------------------------

    def emit(self, event: str, *args: Any, **kwargs: Any) -> bool:
        """Synchronously emit `event` to all matching listeners. Returns True if any fired."""
        fired = False
        once_ids: set[int] = set()

        for info in self._get_listeners(event):
            try:
                info.func(*args, **kwargs)
                fired = True
            except Exception as exc:
                self._handle_error(exc, event, args, kwargs)
            if info.once:
                once_ids.add(info.id)

        for lid in once_ids:
            self._remove_by_id(event, lid)

        return fired

    async def emit_async(self, event: str, *args: Any, **kwargs: Any) -> bool:
        """Asynchronously emit `event` — runs all listeners concurrently."""
        fired = False
        once_ids: set[int] = set()
        listeners = self._get_listeners(event)

        async def run(info: ListenerInfo) -> None:
            nonlocal fired
            try:
                result = info.func(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    await result
                fired = True
            except Exception as exc:
                self._handle_error(exc, event, args, kwargs)
            if info.once:
                once_ids.add(info.id)

        await asyncio.gather(*[run(info) for info in listeners], return_exceptions=True)

        for lid in once_ids:
            self._remove_by_id(event, lid)

        return fired

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def listener_count(self, event: str) -> int:
        """Return number of listeners registered for `event`."""
        return len(self._get_listeners(event))

    def listeners(self, event: str) -> List[ListenerFunc]:
        """Return a copy of the listener list for `event`."""
        return [info.func for info in self._get_listeners(event)]

    def event_names(self) -> List[str]:
        """Return all event names with registered listeners."""
        return list(self._events.keys())

    def raw_listeners(self, event: str) -> List[ListenerFunc]:
        """Return listener functions (same as listeners() here)."""
        return self.listeners(event)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def on_error(self, func: ListenerFunc) -> ListenerFunc:
        """Register a handler for uncaught errors in emit()."""
        self._error_handlers.append(func)
        return func

    def set_max_listeners(self, n: int | None) -> "EventEmitter":
        """Set max listeners before warning (default: unlimited)."""
        self._max_listeners = n
        return self

    def get_max_listeners(self) -> int | None:
        return self._max_listeners

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @classmethod
    def _next_id(cls) -> int:
        with cls._lock:
            cls._LISTENER_ID += 1
            return cls._LISTENER_ID

    def _add_listener(self, event: str, info: ListenerInfo) -> None:
        if self._max_listeners and len(self._events[event]) >= self._max_listeners:
            self._warn_max_listeners(event)
        if info.prepend:
            self._events[event].insert(0, info)
        else:
            self._events[event].append(info)
        self._rebuild_wildcard_cache()

    def _remove_by_id(self, event: str, listener_id: int) -> None:
        self._events[event] = [li for li in self._events[event] if li.id != listener_id]
        self._rebuild_wildcard_cache()

    def _get_listeners(self, event: str) -> List[ListenerInfo]:
        result: List[ListenerInfo] = list(self._events.get(event, []))
        for pattern, infos in self._wildcards.items():
            if self._matches_pattern(event, pattern):
                result.extend(infos)
        return result

    @staticmethod
    def _matches_pattern(event: str, pattern: str) -> bool:
        if pattern == "*":
            return True
        if pattern.endswith("*") and not pattern.endswith("\\*"):
            prefix = pattern[:-1]
            return event.startswith(prefix)
        if pattern.startswith("*"):
            suffix = pattern[1:]
            return event.endswith(suffix)
        return event == pattern

    def _rebuild_wildcard_cache(self) -> None:
        """Rebuild wildcard listener index from _events."""
        self._wildcards.clear()
        for evt, infos in self._events.items():
            if "*" in evt:
                self._wildcards[evt].extend(infos)

    def _handle_error(self, exc: Exception, event: str, args: tuple, kwargs: dict) -> None:
        if self._error_handlers:
            for h in self._error_handlers:
                h(exc, event, *args, **kwargs)
        elif self._events.get("error"):
            self.emit("error", exc)

    def _warn_max_listeners(self, event: str) -> None:
        import sys
        print(
            f"Warning: possible EventEmitter memory leak detected: "
            f"{len(self._events[event]) + 1} listeners for '{event}'. "
            f"Use set_max_listeners(n) to increase limit.",
            file=sys.stderr,
        )


# ----------------------------------------------------------------------
# Thread-safe variant
# ----------------------------------------------------------------------

class ThreadEventEmitter(EventEmitter):
    """
    Thread-safe EventEmitter. All registration and emission is guarded by a lock.
    Safe to use across threads.
    """

    def __init__(self) -> None:
        super().__init__()
        self._thread_lock = threading.RLock()

    def _add_listener(self, event: str, info: ListenerInfo) -> None:
        with self._thread_lock:
            EventEmitter._add_listener(self, event, info)

    def _remove_by_id(self, event: str, listener_id: int) -> None:
        with self._thread_lock:
            EventEmitter._remove_by_id(self, event, listener_id)

    def emit(self, event: str, *args: Any, **kwargs: Any) -> bool:
        with self._thread_lock:
            return EventEmitter.emit(self, event, *args, **kwargs)

    async def emit_async(self, event: str, *args: Any, **kwargs: Any) -> bool:
        with self._thread_lock:
            return await EventEmitter.emit_async(self, event, *args, **kwargs)

    def on(self, event: str, func: ListenerFunc, prepend: bool = False) -> "EventEmitter":
        with self._thread_lock:
            return EventEmitter.on(self, event, func, prepend)

    def once(self, event: str, func: ListenerFunc, prepend: bool = False) -> ListenerFunc:
        with self._thread_lock:
            return EventEmitter.once(self, event, func, prepend)

    def off(self, event: str, func: ListenerFunc | None = None) -> "ThreadEventEmitter":
        with self._thread_lock:
            EventEmitter.off(self, event, func)
        return self

    def remove_all_listeners(self, event: str | None = None) -> "ThreadEventEmitter":
        with self._thread_lock:
            EventEmitter.remove_all_listeners(self, event)
        return self
