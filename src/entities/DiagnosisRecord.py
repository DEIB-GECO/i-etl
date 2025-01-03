from typing import Any

from database.Counter import Counter
from datatypes.Identifier import Identifier
from entities.Record import Record
from enums.Profile import Profile


class DiagnosisRecord(Record):
    def __init__(self, patient_id: Identifier, hospital_id: Identifier,
                 feature_id: Identifier, value: Any, counter: Counter, dataset: str):
        # set up the resource ID
        super().__init__(feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         profile=Profile.DIAGNOSIS, value=value, counter=counter, dataset=dataset)
