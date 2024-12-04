from typing import Any

from database.Counter import Counter
from datatypes.Identifier import Identifier
from entities.Record import Record
from enums.TableNames import TableNames


class ClinicalRecord(Record):
    def __init__(self, id_value: str, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, value: Any, base_id: str,
                 counter: Counter, hospital_name: str, dataset: str):
        # set up the resource ID
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         value=value, resource_type=TableNames.CLINICAL_RECORD,
                         counter=counter, hospital_name=hospital_name, dataset=dataset)

        self.base_id = base_id
