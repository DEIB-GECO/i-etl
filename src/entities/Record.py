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

    # keys to be used when writing JSON or queries
    # those names have to exactly match the variables names declared in entity classes
    SUBJECT_ = "has_subject"
    REG_BY_ = "registered_by"
    INSTANTIATES_ = "instantiates"
    VALUE_ = "value"
    BASE_ID_ = "base_id"
