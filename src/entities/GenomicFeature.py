import dataclasses

from database.Counter import Counter
from entities.OntologyResource import OntologyResource
from entities.Feature import Feature
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.Visibility import Visibility


@dataclasses.dataclass(kw_only=True)
class GenomicFeature(Feature):
    entity_type: str = f"{Profile.GENOMIC}{TableNames.FEATURE}"
