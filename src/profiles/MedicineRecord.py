from datatypes.Identifier import Identifier
from enums.TableNames import TableNames
from profiles.Record import Record
from utils.Counter import Counter


class MedicineRecord(Record):
    def __init__(self, id_value: str, value, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, counter: Counter, hospital_name: str):
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         resource_type=TableNames.MEDICINE_RECORD, value=value, counter=counter, hospital_name=hospital_name)
