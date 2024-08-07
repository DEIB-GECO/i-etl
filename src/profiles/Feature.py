from datatypes.CodeableConcept import CodeableConcept
from enums.DataTypes import DataTypes
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter


class Feature(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, column_type: str, dimension: str, counter: Counter, resource_type: str, hospital_name: str):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.code = code
        self.permitted_datatype = column_type  # no need to check whether the column type is recognisable, we already normalized it while loading the metadata
        self.dimension = dimension
