from datatypes.CodeableConcept import CodeableConcept
from profiles.Feature import Feature
from enums.TableNames import TableNames
from utils.Counter import Counter


class ImagingFeature(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, permitted_datatype: str, counter: Counter):
        # set up the resource ID
        super().__init__(id_value=id_value, code=code, permitted_datatype=permitted_datatype,
                         resource_type=TableNames.IMAGING_FEATURE, counter=counter)
