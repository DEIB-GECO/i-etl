from datatypes.OntologyResource import OntologyResource
from enums.Visibility import Visibility
from entities.Resource import Resource
from database.Counter import Counter


class Feature(Resource):
    def __init__(self, id_value: str, original_name: str, ontology_resource: OntologyResource, column_type: str, dimension: str, counter: Counter,
                 resource_type: str, hospital_name: str, categorical_values: list[OntologyResource], visibility: Visibility):
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.original_name = original_name
        self.ontology_resource = ontology_resource
        self.permitted_datatype = column_type  # no need to check whether the column type is recognisable, we already normalized it while loading the metadata
        self.dimension = dimension
        if categorical_values is not None and len(categorical_values) > 0:
            self.categorical_values = categorical_values  # this is the list of categorical values fot that column
        else:
            self.categorical_values = None  # this avoids to store empty arrays when there is no categorical values for a certain Feature
        self.visibility = visibility
