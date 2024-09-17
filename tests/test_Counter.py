from database.Counter import Counter


class TestCounter:
    def test_constructor(self):
        counter = Counter()
        assert counter.resource_id == 0

    def test_incr_simple(self):
        counter = Counter()
        counter.increment()
        assert counter.resource_id == 1

    def test_incr_multiple(self):
        counter = Counter()
        for _ in range(10):
            counter.increment()
        assert counter.resource_id == 10

    def test_set(self):
        counter = Counter()
        counter.set(new_value=100)
        assert counter.resource_id == 100

        counter.increment()
        assert counter.resource_id == 101

    def test_reset(self):
        counter = Counter()
        counter.increment()
        counter.increment()
        assert counter.resource_id == 2
        counter.reset()
        assert counter.resource_id == 0
