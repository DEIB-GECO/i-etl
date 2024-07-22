import json

import jsonpickle

from utils.setup_logger import log


class Coding:
    def __init__(self, system: str, code: str, display: str):
        log.debug(f"Create a new Coding with ontology being {system}")
        self.system = system  # this is the ontology url, not the ontology name
        self.code = code
        self.display = display

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
