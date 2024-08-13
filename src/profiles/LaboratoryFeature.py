from datatypes.CodeableConcept import CodeableConcept
from enums.TableNames import TableNames
from profiles.Feature import Feature
from utils.Counter import Counter


class LaboratoryFeature(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, category: CodeableConcept,
                 permitted_datatype: str, dimension: str, counter: Counter, hospital_name: str,
                 categorical_values: list[CodeableConcept]):
        # set up the resource ID
        super().__init__(id_value=id_value, code=code, column_type=permitted_datatype, dimension=dimension,
                         resource_type=TableNames.LABORATORY_FEATURE, counter=counter, hospital_name=hospital_name,
                         categorical_values=categorical_values)

        # set up the Lab. feature specific attributes
        self.category = category
