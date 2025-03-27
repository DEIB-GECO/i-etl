import dataclasses

from entities.Resource import Resource
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class Hospital(Resource):
    name: str
    entity_type: str = f"{TableNames.HOSPITAL}"

    # keys to be used when writing JSON or queries
    # those names have to exactly match the variables names declared in entity classes
    NAME_ = "name"

    # keys to be used when using an entity attribute as a query variable
    NAME__ = f"${NAME_}"
