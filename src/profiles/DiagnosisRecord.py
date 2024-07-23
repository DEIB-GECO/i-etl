from typing import Any

from datatypes.Reference import Reference
from enums.TableNames import TableNames
from profiles.Record import Record
from utils.Counter import Counter


class DiagnosisRecord(Record):
    def __init__(self, id_value: str, subject_ref: Reference, hospital_ref: Reference,
                 disease_ref: Reference, value: Any, counter: Counter):
        # set up the resource ID
        super().__init__(id_value=id_value, feature_ref=disease_ref, subject_ref=subject_ref, hospital_ref=hospital_ref,
                         resource_type=TableNames.DIAGNOSIS_RECORD.value, value=value, counter=counter)
