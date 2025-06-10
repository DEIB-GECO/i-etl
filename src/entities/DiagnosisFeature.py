import dataclasses

from entities.Feature import Feature
from enums.Profile import Profile
from enums.TableNames import TableNames


@dataclasses.dataclass()
class DiagnosisFeature(Feature):
    entity_type: str = f"{Profile.DIAGNOSIS}{TableNames.FEATURE}"
