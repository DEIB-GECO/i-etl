from database.Database import Database
from database.Execution import Execution


class Task:
    def __init__(self, database: Database, execution: Execution):
        self.database = database
        self.execution = execution

    def run(self):
        raise NotImplementedError("Each class which inherits from Task should implement a run() method.")
