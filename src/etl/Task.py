from database.Database import Database
from database.Execution import Execution
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics


class Task:
    def __init__(self, database: Database, execution: Execution,
                 quality_stats: QualityStatistics, time_stats: TimeStatistics, dataset_key: str):
        self.database = database
        self.execution = execution
        self.quality_stats = quality_stats
        self.time_stats = time_stats
        self.dataset_key = dataset_key

    def run(self):
        raise NotImplementedError("Each class which inherits from Task should implement a run() method.")
