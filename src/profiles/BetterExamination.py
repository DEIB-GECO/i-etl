from src.datatypes.BetterCodeableConcept import BetterCodeableConcept
from src.profiles.BetterResource import BetterResource
from src.utils.TableNames import TableNames
from src.utils.utils import get_codeable_concept_from_json, get_category_from_json, is_not_nan


class BetterExamination(BetterResource):
    ID_COUNTER = 1

    def __init__(self, id_value: str, code: BetterCodeableConcept, status: str, category: BetterCodeableConcept):
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
            "identifier": self.identifier,
            "resourceType": self.get_type(),
            "code": self.code.to_json(),
            "status": self.status,
            "category": self.category.to_json()
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
