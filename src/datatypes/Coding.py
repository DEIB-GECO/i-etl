import json


class Coding:
    def __init__(self, triple: tuple):
        self.system = triple[0]
        self.code = triple[1]
        self.display = triple[2]

    def to_json(self):
        return {
            "system": str(self.system),
            "code": str(self.code),
            "display": str(self.display)
        }

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
