import dataclasses

from entities.Resource import Resource
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class ResourceTest(Resource):
    entity_type: str = f"{TableNames.TEST}"
