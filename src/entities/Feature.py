import dataclasses

from database.Counter import Counter
from entities.OntologyResource import OntologyResource
from entities.Resource import Resource
from enums.TableNames import TableNames
from enums.Visibility import Visibility


@dataclasses.dataclass(kw_only=True)
class Feature(Resource):
    name: str
    ontology_resource: OntologyResource
    data_type: str
    unit: str
    description: str
    categories: list[OntologyResource]
    visibility: Visibility
    dataset_gid: str
    domain: dict
    entity_type: str

    def __post_init__(self):
        super().__post_init__()

        # set up the feature attributes
        self.datasets = [self.dataset_gid]
        if self.categories is not None and len(self.categories) == 0:
            self.categories = None  # this avoids to store empty arrays when there is no categorical values for a certain Feature
        if self.domain is not None and len(self.domain) == 0:
            self.domain = None
