from typing import Any

from datatypes.Reference import Reference
from profiles.Record import Record
from enums.TableNames import TableNames
from utils.Counter import Counter


class GenomicRecord(Record):
    def __init__(self, id_value: str, value: Any, uri: str, patient_ref: Reference, hospital_ref: Reference,
                 feat_ref: Reference, counter: Counter, hospital_name:str):
        super().__init__(id_value=id_value, feature_ref=feat_ref, patient_ref=patient_ref, hospital_ref=hospital_ref,
                         value=value, resource_type=TableNames.GENOMIC_RECORD, counter=counter, hospital_name=hospital_name)
        # set up specific attributes for genomic records
        self.uri = uri
