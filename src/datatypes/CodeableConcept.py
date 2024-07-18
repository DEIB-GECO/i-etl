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
        Instantiate a new (empty) CodeableConcept
        """
        self._codings = []
        self._text = ""

    def has_codings(self) -> bool:
        return self._codings is not None and len(self._codings) > 0

    def add_coding(self, coding: Coding) -> None:
        if coding is not None:
            self._codings.append(coding)

    def add_codings(self, set_of_dicts: list[dict]) -> None:
        """
        Add a set of new Codings to the list of Codings representing the concept.
        """
        if set_of_dicts is not None:
            for one_dict in set_of_dicts:
                # system is the ontology url, not the ontology name
                self._codings.append(Coding(system=one_dict["system"], code=one_dict["code"], display=one_dict["display"]))

    def to_json(self) -> dict:
        """
        Produce the FHIR-compliant JSON representation of a CodeableConcept.
        :return: A dict being the JSON representation of the CodeableConcept.
        """
        return {
            "text": str(self._text),
            "coding": [coding.to_json() for coding in self._codings]
        }

    @property
    def text(self) -> str:
        return self._text

    @text.setter
    def text(self, new_text: str) -> None:
        self._text = new_text

    @property
    def codings(self):
        return self._codings

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def __repr__(self) -> str:
        return json.dumps(self.to_json())
