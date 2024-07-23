from typing import Any

from datatypes.Reference import Reference
from enums.TableNames import TableNames
from profiles.Record import Record
from utils.Counter import Counter


class ImagingRecord(Record):
    def __init__(self, id_value: str, value: Any, uri: str, subject_ref: Reference, hospital_ref: Reference,
                 feat_ref: Reference, counter: Counter):
        super().__init__(id_value=id_value, feature_ref=feat_ref, subject_ref=subject_ref, hospital_ref=hospital_ref,
                         value=value, resource_type=TableNames.IMAGING_RECORD.value, counter=counter)
        # set up specific attributes for genomic records
        self.uri = uri
