from datetime import datetime

from utils.MetadataColumns import MetadataColumns
from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.TableNames import TableNames
from utils.utils import is_not_nan, get_mongodb_date_from_datetime
from utils.Counter import Counter


class Examination(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, category: CodeableConcept,
                 permitted_data_types: list[str], counter: Counter):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

        # set up the resource attributes
        self.code = code
        self.category = category
        self.permitted_data_types = permitted_data_types

    def get_type(self) -> str:
        return TableNames.EXAMINATION.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "code": self.code.to_json(),
            "category": self.category.to_json(),
            "permittedDatatype": self.permitted_data_types,
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }

    @classmethod
    def get_label(cls, row) -> str:
        display = row[MetadataColumns.COLUMN_NAME.value]
        if is_not_nan(row[MetadataColumns.SIGNIFICATION_EN.value]):
            # by default the display is the variable name
            # if we also have a description, we append it to the display
            # e.g., "BTD (human biotinidase activity)"
            display = display + " (" + str(row[MetadataColumns.SIGNIFICATION_EN.value]) + ")"
        return display
