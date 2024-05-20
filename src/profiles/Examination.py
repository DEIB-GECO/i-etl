from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.utils import build_url, EXAMINATION_TABLE_NAME, assert_variable


class Examination(Resource):
    ID_COUNTER = 1

    def __init__(self, code: CodeableConcept, status: str, category: CodeableConcept, permitted_datatypes: list, multiple_results_allowed: bool, body_site: str):
        super().__init__()

        assert_variable(variable=code, expected_type=CodeableConcept)
        assert_variable(variable=status, expected_type=str)
        assert_variable(variable=category, expected_type=CodeableConcept)

        self.id = Examination.ID_COUNTER
        Examination.ID_COUNTER = Examination.ID_COUNTER + 1
        self.url = build_url(EXAMINATION_TABLE_NAME, self.id)
        self.code = code
        self.status = status
        self.category = category
        self.permitted_datatypes = permitted_datatypes
        self.multiple_results_allowed = multiple_results_allowed
        self.body_site = body_site

    def get_url(self):
        return self.url

    def get_category(self):
        return self.category

    def get_resource_type(self):
        return EXAMINATION_TABLE_NAME

    def to_json(self):
        json_examination = {
            "id": self.id,
            "url": self.url,
            "code": self.code.to_json(),
            "status": self.status,
            "category": self.category.to_json()
        }

        return json_examination
