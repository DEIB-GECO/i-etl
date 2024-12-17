from database.Counter import Counter
from datatypes.Identifier import Identifier
from entities.Record import Record
from enums.Profile import Profile


class MedicineRecord(Record):
    def __init__(self, value, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, counter: Counter, hospital_name: str, dataset: str):
        super().__init__(feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         profile=Profile.MEDICINE, value=value, counter=counter,
                         hospital_name=hospital_name, dataset=dataset)
