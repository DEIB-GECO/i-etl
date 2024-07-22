import json

import jsonpickle

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
        self.coding = []  # coding is a set of Coding - unfortunately FHIR is not consistent with singular/plural...
        self.text = ""

    def has_codings(self) -> bool:
        return self.coding is not None and len(self.coding) > 0

    def add_coding(self, one_coding: Coding) -> None:
        if one_coding is not None:
            self.coding.append(one_coding)

    def add_codings(self, set_of_dicts: list[dict]) -> None:
        """
        Add a set of new Codings to the list of Codings representing the concept.
        """
        if set_of_dicts is not None:
            for one_dict in set_of_dicts:
                # system is the ontology url, not the ontology name
                self.coding.append(Coding(system=one_dict["system"], code=one_dict["code"], display=one_dict["display"]))

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

