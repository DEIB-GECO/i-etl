import dataclasses
import json
from datetime import datetime

from constants.defaults import NO_ID
from constants.methods import factory
from database.Counter import Counter
from database.Operators import Operators


@dataclasses.dataclass(kw_only=True)
class Resource:
    identifier: int
    counter: Counter
    timestamp: dict = dataclasses.field(init=False)

    # keys to be used when writing JSON or queries
    # those names have to exactly match the variables names declared in entity classes
    IDENTIFIER_ = "identifier"
    TIMESTAMP_ = "timestamp"
    ENTITY_TYPE_ = "entity_type"
    DATASET_ = "dataset"
    # keys to be used when using an entity attribute as a query variable
    IDENTIFIER__ = f"${IDENTIFIER_}"
    TIMESTAMP__ = f"${TIMESTAMP_}"
    ENTITY_TYPE__ = f"${ENTITY_TYPE_}"
    DATASET__ = f"${DATASET_}"
    # keys to be used for group by in queries
    DATASET___ = f"$_id.{DATASET_}"

    def __post_init__(self):
        if self.identifier == NO_ID:
            # we are creating a new instance, we assign it a new ID
            self.identifier = self.counter.increment()
        self.timestamp = Operators.from_datetime_to_isodate(current_datetime=datetime.now())

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)

    def __str__(self):
        return json.dumps(self.to_json())
