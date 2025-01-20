import dataclasses

from entities.Record import Record
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class ClinicalRecord(Record):
    base_id: str
    entity_type: str = f"{Profile.CLINICAL}{TableNames.RECORD}"
