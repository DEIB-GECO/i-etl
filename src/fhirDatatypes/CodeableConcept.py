import json

from src.fhirDatatypes.Coding import Coding
from src.utils.setup_logger import log


class CodeableConcept:
    def __init__(self):
        self.text = ""
        self.codings = []

    def add_coding(self, triple: tuple):
        self.codings.append(Coding(triple))

    def to_json(self):
        json_cc = {
            "text": str(self.text),
            "coding": [coding.to_json() for coding in self.codings]
        }
        return json_cc

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
