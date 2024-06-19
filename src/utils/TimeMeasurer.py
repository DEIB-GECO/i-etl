import time


class TimeMeasurer:
    def __init__(self):
        self.measure = 0
        self.start_time = 0
        self.end_time = 0

    def start(self) -> None:
        self.start_time = self.get_ms_time()

    def stop(self) -> None:
        self.end_time = self.get_ms_time()
        self.measure = self.end_time - self.start_time

    def reset(self) -> None:
        self.start_time = 0
        self.end_time = 0
        self.measure = 0

    def reset_and_start(self) -> None:
        self.reset()
        self.start()

    def get_measure(self) -> float:
        return self.measure

    def get_ms_time(self) -> float:
        return round(time.time() * 1000)
