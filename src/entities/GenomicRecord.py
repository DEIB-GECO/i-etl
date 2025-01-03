from typing import Any

from datatypes.Identifier import Identifier
from enums.Profile import Profile
from entities.Record import Record
from database.Counter import Counter


class GenomicRecord(Record):
    def __init__(self, value: Any, vcf: str, patient_id: Identifier, hospital_id: Identifier,
                 feature_id: Identifier, counter: Counter, dataset: str):
        super().__init__(feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         value=value, profile=Profile.GENOMIC, counter=counter, dataset=dataset)
        # set up specific attributes for genomic records
        self.vcf = vcf
