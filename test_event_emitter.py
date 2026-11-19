"""Tests for event_emitter -- the async-first module.

This file had no tests before 1.0.1. All 12 existing tests covered
`tiny_event_emitter`, so every behaviour of this module was unverified --
including the one that was wrong: `emit_async()` dispatched only async
listeners and silently dropped the sync ones, which is exactly the mix the
module's own docstring demonstrates.

Each test names the contract it pins, so a future change that breaks the
contract fails here rather than in a user's agent loop.
"""

import asyncio
import unittest

from event_emitter import Event, EventEmitter, on_async, once, sse_format


def run(coro):
    return asyncio.run(coro)


class TestEventObject(unittest.TestCase):
    def test_listeners_receive_an_event_not_a_payload(self):
        bus = EventEmitter()
        got = []
        bus.on("x", got.append)
        run(bus.emit_async("x", {"a": 1}))
        self.assertEqual(len(got), 1)
        self.assertIsInstance(got[0], Event)
        self.assertEqual(got[0].name, "x")
        self.assertEqual(got[0].data, {"a": 1})

    def test_event_id_is_short_and_unique(self):
        a, b = Event("x"), Event("x")
        self.assertEqual(len(a.id), 8)
        self.assertNotEqual(a.id, b.id)


class TestEmitAsyncDispatch(unittest.TestCase):
    """The regression: emit_async must run BOTH listener kinds."""

    def test_emit_async_runs_sync_listeners(self):
        bus = EventEmitter()
        seen = []
        bus.on("x", lambda e: seen.append("sync"))
        results = run(bus.emit_async("x", 1))
        self.assertEqual(seen, ["sync"])
        self.assertEqual(len(results), 1)

    def test_emit_async_runs_async_listeners(self):
        bus = EventEmitter()
        seen = []

        async def listener(event):
            await asyncio.sleep(0)
            seen.append("async")

        bus.on_async("x", listener)
        run(bus.emit_async("x", 1))
        self.assertEqual(seen, ["async"])

    def test_emit_async_runs_both_kinds(self):
        bus = EventEmitter()
        seen = []
        bus.on("x", lambda e: seen.append("sync"))

        async def listener(event):
            await asyncio.sleep(0)
            seen.append("async")

        bus.on_async("x", listener)
        results = run(bus.emit_async("x", 1))
        self.assertEqual(sorted(seen), ["async", "sync"])
        self.assertEqual(len(results), 2)

    def test_emit_async_awaits_a_sync_listener_returning_a_coroutine(self):
        bus = EventEmitter()
        seen = []

        async def inner():
            await asyncio.sleep(0)
            seen.append("awaited")

        bus.on("x", lambda e: inner())
        run(bus.emit_async("x", 1))
        self.assertEqual(seen, ["awaited"])

    def test_emit_async_on_unknown_event_returns_empty(self):
        self.assertEqual(run(EventEmitter().emit_async("nope")), [])


class TestEmitSync(unittest.TestCase):
    def test_emit_runs_sync_listeners(self):
        bus = EventEmitter()
        seen = []
        bus.on("x", lambda e: seen.append(e.data))
        bus.emit("x", 7)
        self.assertEqual(seen, [7])

    def test_emit_hands_events_to_async_listeners_on_the_loop(self):
        """A sync emit into an async stream is the whole point of a queue
        consumer: the agent loop emits, an SSE writer awaits."""
        bus = EventEmitter()
        seen = []

        async def listener(event):
            seen.append(event.data)

        async def scenario():
            bus.on_async("x", listener)
            bus.emit("x", 1)          # synchronous emit
            await asyncio.sleep(0)     # let the scheduled task run
            return seen

        self.assertEqual(run(scenario()), [1])

    def test_emit_without_a_loop_warns_instead_of_dropping_silently(self):
        import warnings

        bus = EventEmitter()

        async def listener(event):
            pass

        bus.on_async("x", listener)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertEqual(bus.emit("x", 1), [])
        self.assertTrue(
            any("skipped" in str(w.message) for w in caught),
            "a dropped async listener must warn, not vanish",
        )


class TestOnceListeners(unittest.TestCase):
    def test_once_sync_fires_a_single_time(self):
        bus = EventEmitter()
        seen = []
        bus.once("x", lambda e: seen.append(e.data))
        run(bus.emit_async("x", 1))
        run(bus.emit_async("x", 2))
        self.assertEqual(seen, [1])

    def test_once_async_fires_a_single_time(self):
        bus = EventEmitter()
        seen = []

        async def listener(event):
            seen.append(event.data)

        bus.on("x", listener, once=True)
        # on() with once=True routes into the once bucket, which emit_async
        # now dispatches.
        bus.once("y", lambda e: seen.append(e.data))
        run(bus.emit_async("y", 1))
        run(bus.emit_async("y", 2))
        self.assertEqual(seen, [1])


class TestOff(unittest.TestCase):
    def test_off_by_id_removes_one_listener(self):
        bus = EventEmitter()
        seen = []
        lid = bus.on("x", lambda e: seen.append("a"))
        bus.on("x", lambda e: seen.append("b"))
        self.assertTrue(bus.off("x", lid))
        run(bus.emit_async("x"))
        self.assertEqual(seen, ["b"])

    def test_off_all_for_event(self):
        bus = EventEmitter()
        seen = []
        bus.on("x", lambda e: seen.append("a"))
        bus.on("x", lambda e: seen.append("b"))
        bus.off("x")
        run(bus.emit_async("x"))
        self.assertEqual(seen, [])

    def test_off_unknown_id_is_false(self):
        self.assertFalse(EventEmitter().off("x", "nope"))


class TestWildcards(unittest.TestCase):
    def test_pattern_matches_prefix(self):
        bus = EventEmitter()
        seen = []
        bus.on_wildcard("db:*", lambda e: seen.append(e.name))
        for name in ("db:open", "db:write", "cache:miss"):
            bus.emit(name)
        self.assertEqual(seen, ["db:open", "db:write"])

    def test_catch_all(self):
        bus = EventEmitter()
        seen = []
        bus.on_wildcard("*", lambda e: seen.append(e.name))
        bus.emit("a")
        bus.emit("b")
        self.assertEqual(seen, ["a", "b"])

    def test_wildcard_can_be_removed(self):
        bus = EventEmitter()
        seen = []
        bus.on_wildcard("db:*", lambda e: seen.append(e.name))
        bus.off_wildcard("db:*")
        bus.emit("db:open")
        self.assertEqual(seen, [])


class TestIntrospection(unittest.TestCase):
    def test_listener_count(self):
        bus = EventEmitter()
        bus.on("x", lambda e: None)
        bus.on_async("x", lambda e: None)
        self.assertEqual(bus.listener_count("x"), 2)
        self.assertEqual(bus.listener_count(), 2)

    def test_event_names(self):
        bus = EventEmitter()
        bus.on("a", lambda e: None)
        bus.on("b", lambda e: None)
        self.assertEqual(sorted(bus.event_names()), ["a", "b"])

    def test_clear(self):
        bus = EventEmitter()
        bus.on("a", lambda e: None)
        bus.clear()
        self.assertEqual(bus.listener_count(), 0)


class TestWildcardRegistration(unittest.TestCase):
    """on()/on_async() with a pattern string must behave as a pattern.

    Registration stores the key verbatim, so a "db:*" key sat in the
    exact-name bucket and never matched. Only on_wildcard() worked.
    """

    def test_on_with_a_pattern_matches_children(self):
        bus = EventEmitter()
        seen = []
        bus.on("db:*", lambda e: seen.append(e.name))
        bus.emit("db:open")
        bus.emit("db:close")
        bus.emit("cache:miss")
        self.assertEqual(seen, ["db:open", "db:close"])

    def test_on_async_with_a_pattern_matches_children(self):
        bus = EventEmitter()
        seen = []

        async def listener(event):
            seen.append(event.name)

        bus.on_async("db:*", listener)
        run(bus.emit_async("db:open"))
        self.assertEqual(seen, ["db:open"])

    def test_exact_listener_still_fires_alongside_a_pattern(self):
        bus = EventEmitter()
        seen = []
        bus.on("db:*", lambda e: seen.append("pattern"))
        bus.on("db:open", lambda e: seen.append("exact"))
        bus.emit("db:open")
        self.assertEqual(sorted(seen), ["exact", "pattern"])


class TestStreams(unittest.TestCase):
    def test_on_async_streams_events(self):
        async def scenario():
            bus = EventEmitter()
            stream = on_async(bus, "job:*")   # eager: subscribes now

            async def producer():
                for i in range(3):
                    await asyncio.sleep(0)
                    bus.emit(f"job:{i}", {"i": i})

            task = asyncio.create_task(producer())
            got = []
            async for event in stream:
                got.append(event.data["i"])
                if len(got) == 3:
                    break
            await task
            return got

        self.assertEqual(run(scenario()), [0, 1, 2])

    def test_stream_sees_an_event_emitted_before_the_first_iteration(self):
        """The subscription is eager, so nothing is lost between creation
        and the first __anext__. The old lazy version dropped it forever."""

        async def scenario():
            bus = EventEmitter()
            stream = on_async(bus, "job:*")
            bus.emit("job:0", {"i": 0})
            await asyncio.sleep(0)
            return (await asyncio.wait_for(stream.__anext__(), 2)).data["i"]

        self.assertEqual(run(scenario()), 0)

    def test_stream_is_an_async_iterator_not_a_coroutine(self):
        bus = EventEmitter()
        stream = on_async(bus, "x")
        self.assertTrue(hasattr(stream, "__aiter__"),
                        "on_async() must be iterable without await")

    def test_once_stream_yields_one_event(self):
        async def scenario():
            bus = EventEmitter()
            it = once(bus, "ready")        # eager subscription
            bus.emit("ready", "go")
            await asyncio.sleep(0)
            # anext() is 3.10+; the builtin is called explicitly so the
            # suite still runs on the declared 3.8 floor.
            first = await asyncio.wait_for(it.__anext__(), 2)
            return first.data

        self.assertEqual(run(scenario()), "go")


class TestSse(unittest.TestCase):
    def test_frame_shape(self):
        frame = sse_format("tool_call", {"tool": "grep"}, id="abc123")
        self.assertTrue(frame.startswith("event: tool_call\n"))
        self.assertIn('data: {"tool": "grep"}', frame)
        self.assertIn("id: abc123", frame)
        self.assertTrue(frame.endswith("\n\n"))

    def test_frame_without_id(self):
        frame = sse_format("ping", None)
        self.assertNotIn("id:", frame)
        self.assertTrue(frame.endswith("\n\n"))


class TestThreadSafety(unittest.TestCase):
    def test_lock_mode_emits_from_threads(self):
        import threading

        bus = EventEmitter(lock=True)
        seen = []
        bus.on("x", lambda e: seen.append(e.data))

        threads = [threading.Thread(target=bus.emit, args=("x", i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(seen), list(range(20)))


if __name__ == "__main__":
    unittest.main()
