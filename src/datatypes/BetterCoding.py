import json


class BetterCoding:
    def __init__(self, triple: tuple):
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

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
