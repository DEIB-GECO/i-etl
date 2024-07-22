from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class Medicine(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, counter: Counter):
        super().__init__(id_value, TableNames.MEDICINE.value, counter=counter)

        self.code = code
