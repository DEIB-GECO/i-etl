import json

from src.database.TableNames import TableNames
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.setup_logger import log


class Examination(Resource):
    ID_COUNTER = 1

    def __init__(self, code: CodeableConcept, status: str, category: str, permitted_datatypes: list, multiple_results_allowed: bool, body_site: str):
        super().__init__()

        assert code is not None, "The field 'code' is required for Examination, but no value was provided."
        assert status is not None and status != "", "The field 'status' is required for Examination, but not value was provided."
        assert category is not None, "The field 'category' is required for Examination, but no value was provided."

        self.url = Examination.ID_COUNTER
        Examination.ID_COUNTER = Examination.ID_COUNTER + 1
        self.code = code
        self.status = status
        self.category = category
        self.permitted_datatypes = permitted_datatypes
        self.multiple_results_allowed = multiple_results_allowed
        self.body_site = body_site

    def get_url(self):
        return self.url

    def get_code(self):
        return self.code

    def get_status(self):
        return self.status

    def get_category(self):
        return self.category

    def get_permitted_datatypes(self):
        return self.permitted_datatypes

    def get_multiple_results_allowed(self):
        return self.multiple_results_allowed

    def get_body_site(self):
        return self.body_site

    def get_resource_type(self):
        return TableNames.EXAMINATION_TABLE_NAME

    def to_json(self):
        json_examination = {
            "url": str(self.url),
            "code": self.code.to_json(),
            "status": str(self.status),
            "category": str(self.category)
        }
        return json_examination
