from datatypes.CodeableConcept import CodeableConcept
from enums.TableNames import TableNames
from profiles.Feature import Feature
from utils.Counter import Counter
from utils.utils import is_not_nan


class LaboratoryFeature(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, category: CodeableConcept,
                 permitted_datatype: str, counter: Counter):
        # set up the resource ID
        super().__init__(id_value=id_value, code=code, permitted_datatype=permitted_datatype, resource_type=TableNames.LABORATORY_FEATURE.value, counter=counter)

        # set up the Lab. feature specific attributes
        self.category = category

    @classmethod
    def get_display(cls, column_name: str, column_description: str) -> str:
        display = column_name  # row[MetadataColumns.COLUMN_NAME.value]
        if is_not_nan(column_description):  # row[MetadataColumns.SIGNIFICATION_EN.value]):
            # by default the display is the variable name
            # if we also have a description, we append it to the display
            # e.g., "BTD (human biotinidase activity)"
            display = f"{display} ({str(column_description)})"
        return display
