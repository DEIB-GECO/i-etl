from datatypes.CodeableConcept import CodeableConcept
from enums.TableNames import TableNames
from profiles.Feature import Feature
from utils.Counter import Counter


class ImagingFeature(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, permitted_datatype: str, dimension: str, counter: Counter,
                 hospital_name: str, categorical_values: list[CodeableConcept]):
        # set up the resource ID
        super().__init__(id_value=id_value, code=code, column_type=permitted_datatype, dimension=dimension,
                         resource_type=TableNames.IMAGING_FEATURE, counter=counter, hospital_name=hospital_name,
                         categorical_values=categorical_values)
