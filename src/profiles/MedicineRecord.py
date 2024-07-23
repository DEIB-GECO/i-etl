from datatypes.Reference import Reference
from profiles.Record import Record
from enums.TableNames import TableNames
from utils.Counter import Counter


class MedicineRecord(Record):
    def __init__(self, id_value: str, value, medicine_ref: Reference, subject_ref: Reference,
                 hospital_ref: Reference, counter: Counter):
        super().__init__(id_value=id_value, feature_ref=medicine_ref, subject_ref=subject_ref, hospital_ref=hospital_ref,
                         resource_type=TableNames.MEDICINE_RECORD.value, value=value, counter=counter)
