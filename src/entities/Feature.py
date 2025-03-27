import dataclasses

from entities.OntologyResource import OntologyResource
from entities.Resource import Resource
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
    dataset: str
    domain: dict

    # keys to be used when writing JSON or queries
    # those names have to exactly match the variables names declared in entity classes
    NAME_ = "name"
    ONTO_ = "ontology_resource"
    DT_ = "data_type"
    UNIT_ = "unit"
    DESCR_ = "description"
    CATEGORIES_ = "categories"
    VISIBILITY_ = "visibility"
    DOMAIN_ = "domain"

    # keys to be used when using an entity attribute as a query variable
    NAME__ = f"${NAME_}"
    ONTO__ = f"${ONTO_}"
    DT__ = f"${DT_}"
    UNIT__ = f"${UNIT_}"
    DESCR__ = f"${DESCR_}"
    CATEGORIES__ = f"${CATEGORIES_}"
    VISIBILITY__ = f"${VISIBILITY_}"
    DOMAIN__ = f"${DOMAIN_}"

    def __post_init__(self):
        super().__post_init__()

        # set up the feature attributes
        self.datasets = [self.dataset]
        if self.categories is not None and len(self.categories) == 0:
            self.categories = None  # this avoids to store empty arrays when there is no categorical values for a certain Feature
        if self.domain is not None and len(self.domain) == 0:
            self.domain = None
