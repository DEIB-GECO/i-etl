from datatypes.CodeableConcept import CodeableConcept
from profiles.Feature import Feature
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter


class GenomicFeature(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, permitted_datatype: str, dimension: str, counter: Counter, hospital_name: str):
        # set up the resource ID
        super().__init__(id_value=id_value, code=code, column_type=permitted_datatype, dimension=dimension,
                         resource_type=TableNames.GENOMIC_FEATURE, counter=counter, hospital_name=hospital_name)
