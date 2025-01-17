import dataclasses
import json
from datetime import datetime

from constants.methods import factory


@dataclasses.dataclass(kw_only=True)
class Statistics:
    record_stats: bool
    timestamp: datetime = dataclasses.field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.now()

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)

    def __str__(self):
        return json.dumps(self.to_json())
