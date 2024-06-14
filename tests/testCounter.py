import unittest

from src.utils.Counter import Counter


class TestCounter(unittest.TestCase):
    def test_constructor(self):
        counter = Counter()
        self.assertEqual(counter.resource_id, 0)

    def test_incr_simple(self):
        counter = Counter()
        counter.increment()
        self.assertEqual(counter.resource_id, 1)

    def test_incr_multiple(self):
        counter = Counter()
        for _ in range(10):
            counter.increment()
        self.assertEqual(counter.resource_id, 10)

    def test_set(self):
        counter = Counter()
        counter.set(100)
        self.assertEqual(counter.resource_id, 100)

        counter.increment()
        self.assertEqual(counter.resource_id, 101)

    def test_reset(self):
        counter = Counter()
        counter.increment()
        counter.increment()
        self.assertEqual(counter.resource_id, 2)
        counter.reset()
        self.assertEqual(counter.resource_id, 0)