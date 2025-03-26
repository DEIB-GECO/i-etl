import dataclasses
import json

from constants.methods import factory


@dataclasses.dataclass(kw_only=True)
class DatasetProfile:
    description: str
    theme: str
    filetype: str
    size: float  # in Mb
    nb_tuples: int
    completeness: int
    uniqueness: float

    def to_json(self):
        return {"ds_profile": "the_ds_profile"}
        # return dataclasses.asdict(self, dict_factory=factory)

    def __str__(self):
        return json.dumps(self.to_json())
