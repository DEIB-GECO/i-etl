import dataclasses

from entities.Feature import Feature
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass(kw_only=True)
class GenomicFeature(Feature):
    entity_type: str = f"{Profile.GENOMIC}{TableNames.FEATURE}"
