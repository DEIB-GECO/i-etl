import json

from src.fhirDatatypes.Coding import Coding


class CodeableConcept:
    # only one constructor is allowed, so we need to use default values
    def __init__(self, one_coding=None):
        self.text = ""
        if one_coding is None:
            self.codings = []
        else:
            self.codings = [Coding(one_coding)]

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
