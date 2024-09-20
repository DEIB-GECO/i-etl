import time


class Timer:
    def __init__(self):
        self.start_time = 0.0
        self.end_time = 0.0
        self.total_time = 0.0

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.end_time = time.time()
        self.total_time += self.time()
        self.start_time = 0.0
        self.end_time = 0.0

    def reset(self):
        self.start_time = 0.0
        self.end_time = 0.0

    def time(self):
        return self.end_time - self.start_time
