import json

from src.fhirDatatypes.Coding import Coding
from src.utils.setup_logger import log


class CodeableConcept:
    def __init__(self, *kwargs):
        self.text = None
        self.codings = []
        # kwargs is expected to be a list of triplets <system, code, display>
        print(kwargs)
        # for k, v in kwargs.items():
        #     log.debug("%s = %s", k, v)
        #     self.codings.append(Coding(v))

    def __repr__(self):
        json_cc = {
            "text": self.text,
            "codings": [self.codings]
        }
        return json.dumps(json_cc)

    def __str__(self):
        json_cc = {
            "text": self.text,
            "codings": [self.codings]
        }
        return json.dumps(json_cc)