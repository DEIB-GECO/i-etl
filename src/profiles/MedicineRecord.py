from datetime import datetime

from datatypes.Reference import Reference
from profiles.Resource import Resource
from utils.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class MedicineRecord(Resource):
    def __init__(self, id_value: str, quantity, medicine_ref: Reference, patient_ref: Reference,
                 hospital_ref: Reference, counter: Counter):
        super().__init__(id_value, self.get_type(), counter=counter)

        self._quantity = quantity
        self._instantiates = medicine_ref
        self._subject = patient_ref
        self._recorded_by = hospital_ref

    def get_type(self) -> str:
        return TableNames.MEDICINE_RECORD.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "quantity": self._quantity,
            "instantiates": self._instantiates.to_json(),
            "subject": self._subject.to_json(),
            "recordedBy": self._recorded_by.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
