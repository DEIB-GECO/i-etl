import dataclasses

from entities.Resource import Resource
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class Hospital(Resource):
    name: str
    entity_type: str = f"{TableNames.HOSPITAL}"
