import asyncio
import unittest
from tiny_event_emitter import EventEmitter, ThreadEventEmitter


class TestEventEmitter(unittest.TestCase):
    def setUp(self):
        self.emitter = EventEmitter()
        self.calls = []

    def _make_listener(self, name):
        def listener(*args, **kwargs):
            self.calls.append((name, args, kwargs))
        return listener

    def test_on_and_emit(self):
        l1 = self._make_listener("l1")
        self.emitter.on("click", l1)
        self.emitter.emit("click", 1, 2, foo="bar")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0], ("l1", (1, 2), {"foo": "bar"}))

    def test_multiple_listeners(self):
        self.emitter.on("click", self._make_listener("l1"))
        self.emitter.on("click", self._make_listener("l2"))
        self.emitter.emit("click")
        self.assertEqual([c[0] for c in self.calls], ["l1", "l2"])

    def test_emit_returns_true(self):
        self.emitter.on("event", lambda: None)
        self.assertTrue(self.emitter.emit("event"))

    def test_emit_no_listeners_returns_false(self):
        self.assertFalse(self.emitter.emit("silent"))

    def test_once(self):
        self.emitter.once("ready", self._make_listener("once"))
        self.emitter.emit("ready")
        self.assertEqual(len(self.calls), 1)
        self.emitter.emit("ready")
        self.assertEqual(len(self.calls), 1)  # removed after first

    def test_off(self):
        l1 = self._make_listener("l1")
        l2 = self._make_listener("l2")
        self.emitter.on("event", l1)
        self.emitter.on("event", l2)
        self.emitter.off("event", l1)
        self.emitter.emit("event")
        self.assertEqual([c[0] for c in self.calls], ["l2"])

    def test_remove_all_listeners(self):
        self.emitter.on("a", self._make_listener("a"))
        self.emitter.on("b", self._make_listener("b"))
        self.emitter.remove_all_listeners("a")
        self.assertEqual(self.emitter.listener_count("a"), 0)
        self.assertEqual(self.emitter.listener_count("b"), 1)

    def test_listener_count(self):
        self.emitter.on("x", lambda: None)
        self.emitter.on("x", lambda: None)
        self.assertEqual(self.emitter.listener_count("x"), 2)

    def test_listeners(self):
        l1 = lambda: None
        l2 = lambda: None
        self.emitter.on("ev", l1)
        self.emitter.on("ev", l2)
        self.assertEqual(self.emitter.listeners("ev"), [l1, l2])

    def test_event_names(self):
        self.emitter.on("a", lambda: None)
        self.emitter.on("b", lambda: None)
        names = self.emitter.event_names()
        self.assertIn("a", names)
        self.assertIn("b", names)

    def test_prepend(self):
        self.emitter.on("order", self._make_listener("first"))
        self.emitter.on("order", self._make_listener("second"), prepend=True)
        self.emitter.emit("order")
        self.assertEqual([c[0] for c in self.calls], ["second", "first"])

    def test_wildcard_star(self):
        self.emitter.on("user:*", self._make_listener("user_star"))
        self.emitter.emit("user:login")
        self.assertEqual([c[0] for c in self.calls], ["user_star"])

    def test_error_handler(self):
        def bad(*args):
            raise RuntimeError("boom")
        self.emitter.on("fail", bad)
        errors = []
        self.emitter.on_error(lambda exc, ev, *a: errors.append(exc))
        self.emitter.emit("fail")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)

    def test_chaining(self):
        r = self.emitter.on("x", lambda: None).off("x", lambda: None)
        self.assertIs(r, self.emitter)


class TestAsyncEmitter(unittest.TestCase):
    def setUp(self):
        self.emitter = EventEmitter()
        self.results = []

    def test_emit_async(self):
        async def capture(val):
            self.results.append(val)

        async def run():
            self.emitter.on("data", capture)
            await self.emitter.emit_async("data", "hello")
            self.assertEqual(self.results, ["hello"])

        asyncio.run(run())


class TestThreadEventEmitter(unittest.TestCase):
    def test_thread_safety(self):
        import threading
        emitter = ThreadEventEmitter()
        counter = {"value": 0}
        lock = threading.Lock()

        def increment():
            for _ in range(100):
                emitter.emit("tick")
                with lock:
                    counter["value"] += 1

        emitter.on("tick", lambda: None)
        threads = [threading.Thread(target=increment) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(counter["value"], 400)


if __name__ == "__main__":
    unittest.main()
