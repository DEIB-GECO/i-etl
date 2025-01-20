import dataclasses

from entities.Record import Record
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class MedicineRecord(Record):
    entity_type: str = f"{Profile.MEDICINE}{TableNames.RECORD}"
