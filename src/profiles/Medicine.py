from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.TableNames import TableNames


class Medicine(Resource):
    def __init__(self, id_value: str, code: CodeableConcept):
        super().__init__(id_value, self.get_type())

        self.code = code

    def get_type(self):
        return TableNames.MEDICINE.value

    def to_json(self):
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "code": self.code.to_json()
        }
