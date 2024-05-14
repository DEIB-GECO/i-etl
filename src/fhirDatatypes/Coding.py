import json


class Coding:
    def __init__(self, triple: tuple):
        assert type(triple) is tuple, "The 'triple' parameter should be like: <system: str, code: str, display: str>."
        self.system = triple[0]
        self.code = triple[1]
        self.display = triple[2]

    def __repr__(self):
        json_coding = {
            "system": self.system,
            "code": self.code,
            "display": self.display
        }
        return json.dumps(json_coding)

    def __str__(self):
        json_coding = {
            "system": self.system,
            "code": self.code,
            "display": self.display
        }
        return json.dumps(json_coding)
