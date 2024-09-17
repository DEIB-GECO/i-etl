from statistics.Statistics import Statistics


class TimeStatistics(Statistics):
    def __init__(self, record_stats: bool):
        super().__init__(record_stats=record_stats)
