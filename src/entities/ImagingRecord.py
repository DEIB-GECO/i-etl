import dataclasses

from entities.Record import Record
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class ImagingRecord(Record):
    scan: str
    entity_type: str = f"{Profile.IMAGING}{TableNames.RECORD}"
