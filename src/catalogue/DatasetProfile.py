import dataclasses

from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class DatasetProfile:
    description: str
    theme: str
    filetype: str
    size: int
    nb_tuples: int
    completeness: int
    uniqueness: float

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)


def factory(data):
    log.info(data)
    res = {
        key: value
        for (key, value) in data
        if value is not None
    }
    log.info(res)
    return res
