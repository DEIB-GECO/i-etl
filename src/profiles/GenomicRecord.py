from typing import Any

from datatypes.Identifier import Identifier
from enums.TableNames import TableNames
from profiles.Record import Record
from database.Counter import Counter


class GenomicRecord(Record):
    def __init__(self, id_value: str, value: Any, uri: str, patient_id: Identifier, hospital_id: Identifier,
                 feature_id: Identifier, counter: Counter, hospital_name: str):
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         value=value, resource_type=TableNames.GENOMIC_RECORD, counter=counter, hospital_name=hospital_name)
        # set up specific attributes for genomic records
        self.uri = uri
