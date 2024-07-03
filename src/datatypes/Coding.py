import json


class Coding:
    def __init__(self, triple: tuple):
        self._system = triple[0]
        self._code = triple[1]
        self._display = triple[2]

    def to_json(self) -> dict:
        return {
            "system": str(self._system),
            "code": str(self._code),
            "display": str(self._display)
        }

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def __repr__(self) -> str:
        return json.dumps(self.to_json())
