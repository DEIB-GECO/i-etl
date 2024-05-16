import time

class TimeMeasurer:
    def __init__(self):
        self.measure = 0
        self.start_time = 0
        self.end_time = 0

    def start(self):
        self.start_time = self.__get_ms_time()

    def stop(self):
        self.end_time = self.__get_ms_time()
        self.measure = self.end_time - self.start_time

    def reset(self):
        self.start_time = 0
        self.end_time = 0
        self.measure = 0

    def reset_and_start(self):
        self.reset()
        self.start()

    def get_measure(self):
        return self.measure

    def __get_ms_time(self):
        return round(time.time() * 1000)
