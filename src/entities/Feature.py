from constants.defaults import NO_ID
from database.Counter import Counter
from datatypes.OntologyResource import OntologyResource
from entities.Resource import Resource
from enums.TableNames import TableNames
from enums.Visibility import Visibility


class Feature(Resource):
    def __init__(self, name: str, ontology_resource: OntologyResource, column_type: str, unit: str, description: str, counter: Counter,
                 profile: str, categories: list[OntologyResource], visibility: Visibility, dataset_gid: str,
                 domain: dict):
        # set up the resource ID
        super().__init__(id_value=NO_ID, entity_type=f"{profile}{TableNames.FEATURE}", counter=counter)

        # set up the resource attributes
        self.name = name
        self.ontology_resource = ontology_resource
        self.data_type = column_type  # no need to check whether the column type is recognisable, we already normalized it while loading the metadata
        self.unit = unit
        self.description = description
        if categories is not None and len(categories) > 0:
            self.categories = categories  # this is the list of categorical values fot that column
        else:
            self.categories = None  # this avoids to store empty arrays when there is no categorical values for a certain Feature
        self.visibility = visibility
        self.datasets = [dataset_gid]
        self.domain = domain
