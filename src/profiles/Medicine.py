from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class Medicine(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, counter: Counter):
        super().__init__(id_value, self.get_type(), counter=counter)

        self._code = code

    @classmethod
    def get_type(cls) -> str:
        return TableNames.MEDICINE.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "code": self._code.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
