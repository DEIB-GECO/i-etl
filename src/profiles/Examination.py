from src.datatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.utils import get_codeable_concept_from_json, get_category_from_json, is_not_nan


class Examination(Resource):
    ID_COUNTER = 1

    def __init__(self, id_value: str, code: CodeableConcept, category: CodeableConcept,
                 permitted_data_types: list[str]):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.code = code
        self.category = category
        self.permitted_data_types = permitted_data_types

    def get_category(self):
        return self.category

    def get_type(self):
        return TableNames.EXAMINATION.value

    def to_json(self):
        json_examination = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "code": self.code.to_json(),
            "category": self.category.to_json(),
            "permittedDatatype": self.permitted_data_types
        }

        return json_examination

    @classmethod
    def get_label(cls, row) -> str:
        display = row["name"]
        if is_not_nan(row["description"]):
            # by default the display is the variable name
            # if we also have a description, we append it to the display
            # e.g., "BTD (human biotinidase activity)"
            display = display + " (" + str(row["description"]) + ")"
        return display
