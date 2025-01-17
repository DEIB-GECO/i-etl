import dataclasses
import json
from datetime import datetime
from typing import ClassVar

from constants.defaults import NO_ID
from constants.methods import factory
from database.Counter import Counter
from database.Operators import Operators


@dataclasses.dataclass(kw_only=True)
class Resource:
    identifier: int
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
