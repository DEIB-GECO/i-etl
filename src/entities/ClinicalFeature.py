import dataclasses

from entities.Feature import Feature
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass()
class ClinicalFeature(Feature):
    entity_type: str = f"{Profile.CLINICAL}{TableNames.FEATURE}"
