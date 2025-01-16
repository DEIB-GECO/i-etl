import dataclasses
from datetime import datetime

from constants.defaults import NO_ID
from database.Counter import Counter
from database.Database import Database
from database.Operators import Operators
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics


@dataclasses.dataclass(kw_only=True)
class Resource:
    identifier: int
    # entity_type: str # this needs to be part of any child class
    counter: Counter

    def __post_init__(self):
        if self.identifier == NO_ID:
            # we are creating a new instance, we assign it a new ID
            self.identifier = self.counter.increment()
        self.timestamp = Operators.from_datetime_to_isodate(current_datetime=datetime.now())
        self.counter = None

    def to_json(self):
        return dataclasses.asdict(
            self,
            dict_factory=lambda fields: {
                key: Operators.from_datetime_to_isodate(value) if isinstance(value, datetime) else value
                for (key, value) in fields
                if value is not None and not isinstance(value, Counter) and not isinstance(value, QualityStatistics) and not isinstance(value, TimeStatistics) and not isinstance(value, Database)
            }
        )
