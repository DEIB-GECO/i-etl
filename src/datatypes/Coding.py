import jsonpickle

from utils.setup_logger import log
from utils.utils import is_not_nan, normalize_ontology_code, get_display


class Coding:
    def __init__(self, system: str, code: str, name: str, description: str):
        if not is_not_nan(value=system):
            # no ontology code has been provided for that variable name, let's skip it
            log.error("Could not create a Coding with no ontology resource.")
            self.system = None
            self.code = None
            self.display = None
        else:
            self.system = system  # this is the ontology url, not the ontology name
            self.code = normalize_ontology_code(ontology_code=code)
            self.display = get_display(name=name, description=description)
            log.debug(f"Create a new Coding for {self.system}/{self.code}, labelled {self.display}")

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
