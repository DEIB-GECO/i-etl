from datatypes.CodeableConcept import CodeableConcept
from enums.TableNames import TableNames
from profiles.Feature import Feature
from utils.Counter import Counter


class Medicine(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, permitted_datatype: str, counter: Counter):
        super().__init__(id_value=id_value, code=code, permitted_datatype=permitted_datatype, resource_type=TableNames.MEDICINE_FEATURE, counter=counter)
