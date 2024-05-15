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
            "system": str(self.system),
            "code": str(self.code),
            "display": str(self.display)
        }
        return json_coding
