import dataclasses
import time

from statistics.Statistics import Statistics
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class TimeStatistics(Statistics):
    stats: dict = dataclasses.field(default_factory=dict)

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

    def count(self, value: int, dataset: str | None, key: str):
        if dataset is None:
            dataset = "ALL"
        if dataset not in self.stats:
            self.stats[dataset] = {}
        if key in self.stats[dataset]:
            self.stats[dataset][key]["count"] += value
        else:
            self.stats[dataset][key] = {}
            self.stats[dataset][key]["count"] = value
