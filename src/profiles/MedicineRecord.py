from datetime import datetime

from src.datatypes.Reference import Reference
from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter
from src.utils.utils import get_mongodb_date_from_datetime


class MedicineRecord(Resource):
    def __init__(self, id_value: str, quantity, medicine_ref: Reference, patient_ref: Reference,
                 hospital_ref: Reference, counter: Counter):
        super().__init__(id_value, self.get_type(), counter=counter)

        self.quantity = quantity
        self.instantiates = medicine_ref
        self.subject = patient_ref
        self.recorded_by = hospital_ref

    def get_type(self) -> str:
        return TableNames.MEDICINE_RECORD.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "quantity": self.quantity,
            "instantiates": self.instantiates.to_json(),
            "subject": self.subject.to_json(),
            "recordedBy": self.recorded_by.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
