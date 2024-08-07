import jsonpickle

from collections import Counter
from datatypes.Coding import Coding
from enums.Ontologies import Ontologies
from utils.setup_logger import log
from utils.utils import normalize_column_name, normalize_ontology_system, normalize_ontology_code


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

    @classmethod
    def create_without_row(cls, ontology1: str, code1: str, ontology2: str, code2: str, column_name: str, column_description: str):
        cc = CodeableConcept()

        column_name = normalize_column_name(column_name=column_name)
        ontology1 = normalize_ontology_system(ontology_system=ontology1)
        ontology1 = Ontologies.get_ontology_system(ontology=ontology1)  # get the URI of the ontology system instead of its string name
        code1 = normalize_ontology_code(ontology_code=code1)  # get the ontology code in the metadata for the given column and normalize it (just in case)
        cc.add_coding(one_coding=Coding(system=ontology1,
                                        code=code1,
                                        name=column_name,
                                        description=column_description))

        if ontology2 is not None:
            ontology2 = normalize_ontology_system(ontology_system=ontology2)
            ontology2 = Ontologies.get_ontology_system(ontology=ontology2)
            code2 = normalize_ontology_code(ontology_code=code2)
            cc.add_coding(one_coding=Coding(system=ontology2,
                                            code=code2,
                                            name=column_name,
                                            description=column_description))
        cc.text = column_name
        log.debug(cc.coding)
        return cc

    def __eq__(self, other):
        if not isinstance(other, CodeableConcept):
            raise TypeError(f"Could not compare the current instance with an instance of type {type(other)}.")

        return self.text == other.text and sorted(self.coding) == sorted(other.coding)

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

