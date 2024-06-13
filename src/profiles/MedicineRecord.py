from datatypes.Reference import Reference
from profiles.Resource import Resource
from utils.TableNames import TableNames


class MedicineRecord(Resource):
    def __init__(self, id_value: str, quantity, medicine_ref: Reference, patient_ref: Reference,
                 hospital_ref: Reference):
        super().__init__(id_value, self.get_type())

        self.quantity = quantity
        self.instantiates = medicine_ref
        self.subject = patient_ref
        self.recorded_by = hospital_ref

    def get_type(self):
        return TableNames.MEDICINE_RECORD.value

    def to_json(self):
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "quantity": self.quantity,
            "instantiates": self.instantiates.to_json(),
            "subject": self.subject.to_json(),
            "recordedBy": self.recorded_by.to_json()
        }
