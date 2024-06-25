from src.database.Database import Database
from src.utils.setup_logger import log


class Counter:
    def __init__(self):
        self.resource_id = 0

    def increment(self) -> int:
        self.resource_id = self.resource_id + 1
        return self.resource_id

    def set(self, new_value) -> None:
        self.resource_id = new_value

    def set_with_database(self, database: Database) -> None:
        max_value = database.get_resource_counter_id()
        # Resource.set_counter(max_value + 1)  # start 1 after the current counter to avoid resources with the same ID
        if max_value > -1:
            log.debug("will set the counter with %s", max_value)
            self.set(max_value)

    def reset(self) -> None:
        self.resource_id = 0
