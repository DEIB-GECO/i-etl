import dataclasses
from typing import Any

from entities.Resource import Resource


@dataclasses.dataclass(kw_only=True)
class Record(Resource):
    has_subject: int
    registered_by: int
    instantiates: int
    value: Any
    dataset: str
