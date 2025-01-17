import dataclasses
import json
from datetime import datetime, date, time

from constants.defaults import NO_ID
from database.Database import Database
from statistics.DatabaseStatistics import DatabaseStatistics
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from database.Counter import Counter
from database.Operators import Operators
from utils.setup_logger import log


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

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)

    def __str__(self):
        return json.dumps(self.to_json())

def factory(data):
    log.info(data)
    res = {
        key: Operators.from_datetime_to_isodate(value) if isinstance(value, (datetime, date, time)) else value
        for (key, value) in data
        if value is not None and not isinstance(value, (Database, Counter, QualityStatistics, TimeStatistics, DatabaseStatistics))
    }
    log.info(res)
    return res
