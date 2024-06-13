import json

from src.datatypes.Coding import Coding


class CodeableConcept:
    """
    The class CodeableConcept implements the FHIR CodeableConcept data type.
    This allows to groups different representations of a single concept, e.g.,
    LOINC/123 and SNOMED/456 both represent the eye color feature. Each representation is called a Coding,
    and is composed of a system (LOINC, SNOMED, ...) and a value (123, 456, ...).
    For more details about Codings, see the class Coding.
    """
    def __init__(self, one_coding: tuple = None, text: str = ""):
        """
        Instantiate a new CodeableConcept, empty or with a coding (represented as a tuple).
        The text is left empty for now.
        :param one_coding: A tuple being the coding representing a concept. It is of the form: (system, code, display).
        """
        self.text = text
        if one_coding is None:
            self.codings = []
        else:
            self.codings = [Coding(one_coding)]

    def add_coding(self, triple: tuple) -> None:
        """
        Add a new Coding to the list of Codings representing the concept.
        :param triple: A tuple being the coding representing a concept. It is of the form: (system, code, display).
        :return: Nothing.
        """
        if triple is not None:
            self.codings.append(Coding(triple))

    def to_json(self) -> dict:
        """
        Produce the FHIR-compliant JSON representation of a CodeableConcept.
        :return: A dict being the JSON representation of the CodeableConcept.
        """
        json_cc = {
            "text": str(self.text),
            "coding": [coding.to_json() for coding in self.codings]
        }
        return json_cc

    @classmethod
    def from_json(cls, the_json: dict):
        cc = cls(one_coding=None, text=the_json["text"])
        for one_coding in the_json["codings"]:
            cc.add_coding((one_coding["system"], one_coding["value"], one_coding["display"]))
        return cc

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
