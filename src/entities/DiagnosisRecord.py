from typing import Any

from database.Counter import Counter
from datatypes.Identifier import Identifier
from entities.Record import Record
from enums.Profile import Profile
from utils.setup_logger import log


class DiagnosisRecord(Record):
    def __init__(self, patient_id: Identifier, hospital_id: Identifier,
                 feature_id: Identifier, value: Any, diagnosis_counter: int, counter: Counter, dataset: str):
        # set up the resource ID
        super().__init__(feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         profile=Profile.DIAGNOSIS, value=value, counter=counter, dataset=dataset)
        self.diagnosis_counter = diagnosis_counter
        # log.info(f"DiagnosisRecord with counter {self.diagnosis_counter}")
