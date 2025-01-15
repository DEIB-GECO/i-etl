import time

from statistics.Statistics import Statistics
from utils.setup_logger import log


class TimeStatistics(Statistics):
    def __init__(self, record_stats: bool):
        super().__init__(record_stats=record_stats)

        self.stats = {}

    def start(self, dataset: str | None, key: str):
        if dataset is None:
            dataset = "ALL"
        if dataset not in self.stats:
            self.stats[dataset] = {}
        if key not in self.stats[dataset]:
            self.stats[dataset][key] = {"start_time": 0.0, "cumulated_time": 0.0}
        self.stats[dataset][key]["start_time"] = time.time()

    def increment(self, dataset: str | None, key: str):
        if dataset is None:
            dataset = "ALL"
        if dataset in self.stats and key in self.stats[dataset]:
            self.stats[dataset][key]["cumulated_time"] += time.time() - self.stats[dataset][key]["start_time"]
        else:
            log.error(f"No existing timer for dataset {dataset} and key {key}")

    def to_json(self):
        return self.stats

    def __getstate__(self):
        # to directly show the timings in the DB, instead of nested objects containing timings
        return self.to_json()
