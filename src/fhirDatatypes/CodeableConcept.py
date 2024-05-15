import json

from src.fhirDatatypes.Coding import Coding
from src.utils.setup_logger import log


class CodeableConcept:
    def __init__(self, *kwargs):
        self.text = ""
        self.codings = []
        # kwargs is expected to be a list of triplets <system, code, display>
        for triple in kwargs:
            self.codings.append(Coding(triple))

    def to_json(self):
        json_cc = {
            "text": self.text,
            "codings": [coding.to_json() for coding in self.codings]
        }
        return json_cc

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
