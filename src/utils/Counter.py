from database.Database import Database
from utils.setup_logger import log


class Counter:
    def __init__(self):
        self.resource_id = 0

    def increment(self) -> int:
        self.resource_id = self.resource_id + 1
        return self.resource_id

    def set(self, new_value) -> None:
        self.resource_id = new_value

    def set_with_database(self, database: Database) -> None:
        max_value = database.get_max_resource_counter_id() + 1  # start 1 after the current counter to avoid resources with the same ID
        if max_value > -1:
            self.set(max_value)
            log.debug(f"The resource counter is now set to {self.resource_id}.")

    def reset(self) -> None:
        self.resource_id = 0
