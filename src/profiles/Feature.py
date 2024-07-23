from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter


class Feature(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, permitted_datatype: str, counter: Counter, resource_type: str):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter)

        # set up the resource attributes
        self.code = code
        self.permitted_datatype = permitted_datatype  # TODO Nelly: assert that datatype is a "real" data type
