from src.utils.setup_logger import log


class Counter:
    def __init__(self):
        self.resource_id = 0

    def increment(self) -> int:
        self.resource_id = self.resource_id + 1
        return self.resource_id

    def set(self, new_value):
        self.resource_id = new_value

    def reset(self):
        self.resource_id = 0
