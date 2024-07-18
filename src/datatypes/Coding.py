import json

from utils.setup_logger import log


class Coding:
    def __init__(self, system: str, code: str, display: str):
        log.debug(f"Create a new Coding with ontology being {system}")
        self._system = system  # this is the ontology url, not the ontology name
        self._code = code
        self._display = display

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

    @property
    def system(self):
        return self._system

    @property
    def code(self):
        return self._code

    @property
    def display(self):
        return self._display
