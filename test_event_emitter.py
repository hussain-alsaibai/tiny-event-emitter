"""Tests for tiny-event-emitter."""
import pytest
from event_emitter import EventEmitter, Event, sse_format


class TestBasic:
    def test_emit_and_listen(self):
        emitter = EventEmitter()
        results = []

        def handler(event):
            results.append(event.data)

        emitter.on("data", handler)
        emitter.emit("data", {"hello": "world"})

        assert results == [{"hello": "world"}]

    def test_multiple_listeners(self):
        emitter = EventEmitter()
        results = []

        emitter.on("data", lambda e: results.append(1))
        emitter.on("data", lambda e: results.append(2))
        emitter.emit("data", None)

        assert results == [1, 2]

    def test_emit_returns_results(self):
        emitter = EventEmitter()
        emitter.on("data", lambda e: 42)
        emitter.on("data", lambda e: 99)
        results = emitter.emit("data", None)

        assert set(results) == {42, 99}


class TestOnce:
    def test_once_fires_once(self):
        emitter = EventEmitter()
        results = []

        emitter.once("connect", lambda e: results.append(1))
        emitter.emit("connect", None)
        emitter.emit("connect", None)

        assert results == [1]

    def test_on_with_once_flag(self):
        emitter = EventEmitter()
        results = []

        emitter.on("data", lambda e: results.append(1), once=True)
        emitter.emit("data", None)
        emitter.emit("data", None)

        assert results == [1]


class TestWildcards:
    def test_star_wildcard(self):
        emitter = EventEmitter()
        results = []

        emitter.on_wildcard("user:*", lambda e: results.append(e.name))
        emitter.emit("user:login", None)
        emitter.emit("user:logout", None)
        emitter.emit("post:create", None)  # shouldn't match

        assert results == ["user:login", "user:logout"]


class TestRemove:
    def test_off_by_id(self):
        emitter = EventEmitter()
        results = []

        lid = emitter.on("data", lambda e: results.append(1))
        emitter.on("data", lambda e: results.append(2))
        emitter.off("data", lid)
        emitter.emit("data", None)

        assert results == [2]


class TestListenerCount:
    def test_count(self):
        emitter = EventEmitter()
        assert emitter.listener_count("data") == 0

        emitter.on("data", lambda e: None)
        emitter.on("data", lambda e: None)
        assert emitter.listener_count("data") == 2

    def test_total_count(self):
        emitter = EventEmitter()
        emitter.on("a", lambda e: None)
        emitter.on("b", lambda e: None)
        emitter.on("c", lambda e: None)
        assert emitter.listener_count() == 3


class TestEventNames:
    def test_event_names(self):
        emitter = EventEmitter()
        emitter.on("login", lambda e: None)
        emitter.on("logout", lambda e: None)
        assert set(emitter.event_names()) == {"login", "logout"}


class TestSSE:
    def test_sse_format(self):
        output = sse_format("update", {"counter": 42}, id="msg-1")
        assert "event: update" in output
        assert '"counter": 42' in output
        assert "id: msg-1" in output


class TestClear:
    def test_clear(self):
        emitter = EventEmitter()
        emitter.on("data", lambda e: None)
        emitter.on("data", lambda e: None)
        emitter.clear()
        assert emitter.listener_count() == 0
