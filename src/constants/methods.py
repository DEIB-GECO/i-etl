from datetime import date, time, datetime

from database.Operators import Operators
from utils.setup_logger import log


def factory(data):
    # we cannot do the following because dataclasses are considering nested fields as json dictionaries (they are not Python classes anymore)
    # instead, we can check the names of the variables (not very good, but still better than nothing)
    # if not isinstance(value, (Database, Counter, QualityStatistics, TimeStatistics, DatabaseStatistics)):
    # if value is not None and key not in ["quality_stats", "time_stats", "database_stats", "counter", "database", "execution"]:
    # log.info(data)
    return {
        key: Operators.from_datetime_to_isodate(value) if isinstance(value, (datetime, date, time)) else value
        for (key, value) in data
        if value is not None and value != [] and value != {} and key not in ["quality_stats", "time_stats", "database_stats", "counter", "database", "execution", "client", "db", "dataset_key"]
    }
