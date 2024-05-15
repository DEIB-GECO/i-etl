import json

from src.utils.setup_logger import log


class Coding:
    def __init__(self, triple: tuple):
        assert type(triple) is tuple, "The 'triple' parameter should be like: <system: str, code: str, display: str>."
        self.system = triple[0]
        self.code = triple[1]
        self.display = triple[2]

    def to_json(self):
        json_coding = {
            "system": self.system,
            "code": self.code,
            "display": self.display
        }
        log.debug(json_coding)
        return json_coding
