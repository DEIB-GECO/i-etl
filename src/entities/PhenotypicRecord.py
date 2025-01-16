from entities.Record import Record
from enums.Profile import Profile
from enums.TableNames import TableNames


class PhenotypicRecord(Record):
    entity_type: str = f"{Profile.PHENOTYPIC}{TableNames.RECORD}"
