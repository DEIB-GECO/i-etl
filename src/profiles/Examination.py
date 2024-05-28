from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.utils import NONE_VALUE


class Examination(Resource):
    ID_COUNTER = 1

    def __init__(self, id_value: str, code: CodeableConcept, status: str, category: CodeableConcept):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.code = code
        self.status = status
        self.category = category

    def get_category(self):
        return self.category

    def get_type(self):
        return TableNames.EXAMINATION.value

    def to_json(self):
        json_examination = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "code": self.code.to_json(),
            "status": self.status,
            "category": self.category.to_json()
        }

        return json_examination
