import dataclasses
import json
from datetime import datetime

from database.Database import Database
from database.Execution import Execution


@dataclasses.dataclass(kw_only=True)
class Statistics:
    record_stats: bool
    timestamp: datetime = dataclasses.field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.now()

    def to_json(self):
        return dataclasses.asdict(
            self,
            dict_factory=lambda fields: {
                key: value
                for (key, value) in fields
                if value is not None and type(value) not in [Database, Execution]
            },
        )
