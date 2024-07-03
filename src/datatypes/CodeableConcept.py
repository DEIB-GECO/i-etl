import json

from datatypes.Coding import Coding


class CodeableConcept:
    """
    The class CodeableConcept implements the FHIR CodeableConcept data type.
    This allows to groups different representations of a single concept, e.g.,
    LOINC/123 and SNOMED/456 both represent the eye color feature. Each representation is called a Coding,
    and is composed of a system (LOINC, SNOMED, ...) and a value (123, 456, ...).
    For more details about Codings, see the class Coding.
    """
    def __init__(self):
        """
        Instantiate a new CodeableConcept, empty or with a coding (represented as a tuple).
        """
        self._codings = []
        self._text = ""

    def has_no_coding(self) -> bool:
        return self._codings is None or self._codings == []

    def add_coding(self, triple: tuple) -> None:
        """
        Add a new Coding to the list of Codings representing the concept.
        :param triple: A tuple being the coding representing a concept. It is of the form: (system, code, display).
        :return: Nothing.
        """
        if triple is not None:
            self._codings.append(Coding(triple=triple))

    def to_json(self) -> dict:
        """
        Produce the FHIR-compliant JSON representation of a CodeableConcept.
        :return: A dict being the JSON representation of the CodeableConcept.
        """
        return {
            "text": str(self._text),
            "coding": [coding.to_json() for coding in self._codings]
        }

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def __repr__(self) -> str:
        return json.dumps(self.to_json())
