from statistics.Statistics import Statistics
from statistics.Timer import Timer


class TimeStatistics(Statistics):
    def __init__(self, record_stats: bool):
        super().__init__(record_stats=record_stats)

        self.total_execution_timer = Timer()
        self.total_extract_timer = Timer()
        self.total_transform_timer = Timer()
        self.total_load_timer = Timer()

    def start_total_execution_timer(self):
        self.total_execution_timer.start()

    def start_total_extract_timer(self):
        self.total_extract_timer.start()

    def start_total_transform_timer(self):
        self.total_transform_timer.start()

    def start_total_load_timer(self):
        self.total_load_timer.start()

    def stop_total_execution_timer(self):
        self.total_execution_timer.stop()

    def stop_total_extract_timer(self):
        self.total_extract_timer.stop()

    def stop_total_transform_timer(self):
        self.total_transform_timer.stop()

    def stop_total_load_timer(self):
        self.total_load_timer.stop()

    def to_json(self):
        return {  # TODO: do a for loop to iterate over the timers and get their total time for each
            "total_execution_time": self.total_execution_timer.total_time,
            "total_extract_time": self.total_extract_timer.total_time,
            "total_transform_time": self.total_transform_timer.total_time,
            "total_load_time": self.total_load_timer.total_time
        }

    def __getstate__(self):
        # to directly show the timings in the DB, instead of nested objects containing timings
        return self.to_json()
