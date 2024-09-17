from datatypes.CodeableConcept import CodeableConcept
from enums.Visibility import Visibility
from profiles.Resource import Resource
from database.Counter import Counter


class Feature(Resource):
    def __init__(self, id_value: str, code: CodeableConcept, column_type: str, dimension: str, counter: Counter,
                 resource_type: str, hospital_name: str, categorical_values: list[CodeableConcept], visibility: Visibility):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.code = code
        self.permitted_datatype = column_type  # no need to check whether the column type is recognisable, we already normalized it while loading the metadata
        self.dimension = dimension
        if categorical_values is not None and len(categorical_values) > 0:
            self.categorical_values = categorical_values  # this is the list of categorical values fot that column
        else:
            self.categorical_values = None  # this avoids to store empty arrays when there is no categorical values for a certain Feature
        self.visibility = visibility
