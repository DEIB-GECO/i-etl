import dataclasses

from entities.Record import Record
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass()
class DiagnosisRecord(Record):
    diagnosis_counter: int
    entity_type: str = f"{Profile.DIAGNOSIS}{TableNames.RECORD}"
