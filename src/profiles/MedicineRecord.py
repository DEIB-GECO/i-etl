from datatypes.Reference import Reference
from profiles.Record import Record
from enums.TableNames import TableNames
from utils.Counter import Counter


class MedicineRecord(Record):
    def __init__(self, id_value: str, value, medicine_ref: Reference, patient_ref: Reference,
                 hospital_ref: Reference, counter: Counter, hospital_name: str):
        super().__init__(id_value=id_value, feature_ref=medicine_ref, patient_ref=patient_ref, hospital_ref=hospital_ref,
                         resource_type=TableNames.MEDICINE_RECORD, value=value, counter=counter, hospital_name=hospital_name)
