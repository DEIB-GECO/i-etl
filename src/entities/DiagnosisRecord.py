from typing import Any

from datatypes.Identifier import Identifier
from enums.TableNames import TableNames
from entities.Record import Record
from database.Counter import Counter


class DiagnosisRecord(Record):
    def __init__(self, id_value: str, patient_id: Identifier, hospital_id: Identifier,
                 feature_id: Identifier, value: Any, counter: Counter,
                 hospital_name: str, dataset_name: str):
        # set up the resource ID
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         resource_type=TableNames.DIAGNOSIS_RECORD, value=value,
                         counter=counter, hospital_name=hospital_name, dataset_name=dataset_name)
