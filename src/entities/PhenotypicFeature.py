import dataclasses

from entities.Feature import Feature
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class PhenotypicFeature(Feature):
    entity_type: str = f"{Profile.PHENOTYPIC}{TableNames.FEATURE}"
