import dataclasses
from datetime import datetime, date, time

from database.Counter import Counter
from database.Database import Database
from database.Operators import Operators
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class Statistics:
    record_stats: bool
    timestamp: datetime = dataclasses.field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.now()

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)


# the factory has to be redefined and simplified here to avoid circular dependencies
def factory(data):
    log.info(data)
    res = {
        key: Operators.from_datetime_to_isodate(value) if isinstance(value, (datetime, date, time)) else value
        for (key, value) in data
        if value is not None and not isinstance(value, (Database, Counter))
    }
    log.info(res)
    return res
