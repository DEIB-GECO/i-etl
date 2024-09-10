import jsonpickle

from datatypes.OntologyResource import OntologyResource
from utils.setup_logger import log
from utils.utils import is_not_nan


class Coding:
    def __init__(self, code: OntologyResource, display: str|None):
        if not is_not_nan(value=code.full_code):
            # no ontology code has been provided for that variable name, let's skip it
            log.error("Could not create a Coding with no ontology code.")
            self.system = None
            self.code = None
            self.display = None
        else:
            # this corresponds to the first (and only) ontology system;
            # if there are many, we record only the first but make API calls with all
            # this is because a FHIR Coding can have a single system
            # TODO Nelly: maybe there is a better way?
            self.system = code.ontology["url"]
            code.compute_concat_codes()
            self.code = code.concat_codes
            if display is None:
                # when we create a new CodeableConcept from scratch, we need to compute the display with ontology API
                # if the query to the API does not work, we can still use the column name as the display of the coding
                # the column name is stored in display to avoid adding another parameter to the Coding constructor
                code.compute_concat_names()
                self.display = code.concat_names
            else:
                # when we retrieve a CodeableConcept from the db, we do NOT want to compute again its display
                # we still get the existing display
                self.display = display
            # log.debug(f"Create a new Coding for {self.system}/{self.code}, labelled {self.display}")

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __eq__(self, other):
        if not isinstance(other, Coding):
            raise TypeError(f"Could not compare the current instance with an instance of type {type(other)}.")

        # we do not use the display  because this would lead to unequal instances
        # if provided descriptions differ from one hospital to another
        return self.system == other.system and self.code == other.code

    # def __hash__(self):
    #     # we do not use the display in the hash
    #     # because this would lead to different hashes
    #     # if provided descriptions differ from one hospital to another
    #     my_hash = hash((self.system, self.code))
    #     log.debug(f"compute hash of {self.system} and {self.code}: {my_hash}")
    #     return my_hash

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
