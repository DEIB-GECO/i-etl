from datatypes.Identifier import Identifier
from enums.TableNames import TableNames
from entities.Record import Record
from database.Counter import Counter


class MedicineRecord(Record):
    def __init__(self, id_value: str, value, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, counter: Counter, hospital_name: str, dataset_name: str):
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         resource_type=TableNames.MEDICINE_RECORD, value=value, anonymized_value=None, counter=counter,
                         hospital_name=hospital_name, dataset_name=dataset_name)
