from datetime import datetime

from datatypes.Reference import Reference
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class MedicineRecord(Resource):
    def __init__(self, id_value: str, quantity, medicine_ref: Reference, patient_ref: Reference,
                 hospital_ref: Reference, counter: Counter):
        super().__init__(id_value, TableNames.MEDICINE_RECORD.value, counter=counter)

        self.quantity = quantity
        self.instantiates = medicine_ref
        self.subject = patient_ref
        self.recorded_by = hospital_ref
