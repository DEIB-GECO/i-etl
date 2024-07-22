from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.utils import is_not_nan, get_mongodb_date_from_datetime
from utils.Counter import Counter


class Examination(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, category: CodeableConcept,
                 permitted_datatype: str, counter: Counter):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=TableNames.EXAMINATION.value, counter=counter)

        # set up the resource attributes
        self.code = code
        self.category = category
        self.permitted_datatype = permitted_datatype  # TODO Nelly: assert that datatype is a "real" data type

    @classmethod
    def get_display(cls, column_name: str, column_description: str) -> str:
        display = column_name  # row[MetadataColumns.COLUMN_NAME.value]
        if is_not_nan(column_description):  # row[MetadataColumns.SIGNIFICATION_EN.value]):
            # by default the display is the variable name
            # if we also have a description, we append it to the display
            # e.g., "BTD (human biotinidase activity)"
            display = f"{display} ({str(column_description)})"
        return display
