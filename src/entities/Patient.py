import dataclasses

from entities.Resource import Resource
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class Patient(Resource):
    entity_type: str = f"{TableNames.PATIENT}"
