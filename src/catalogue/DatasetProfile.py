import dataclasses
import json
from datetime import datetime

from database.Database import Database
from database.Operators import Operators


@dataclasses.dataclass(kw_only=True)
class DatasetProfile:
    def __init__(self, description: str, theme: str, filetype: str, size: int, nb_tuples: int, completeness: int, uniqueness: float):
        self.description = description
        self.theme = theme
        self.filetype = filetype
        self.size = size
        self.nb_tuples = nb_tuples
        self.completeness = completeness
        self.uniqueness = uniqueness

    def to_json(self):
        return dataclasses.asdict(
            self,
            dict_factory=lambda fields: {
                key: Operators.from_datetime_to_isodate(value) if isinstance(value, datetime) else value
                for (key, value) in fields
                if value is not None
            },
        )

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def __repr__(self) -> str:
        return json.dumps(self.to_json())
