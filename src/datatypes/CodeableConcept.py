import jsonpickle

from datatypes.Coding import Coding
from enums.Ontologies import Ontologies
from utils.setup_logger import log
from utils.utils import normalize_column_name, normalize_ontology_name, normalize_ontology_code


class CodeableConcept:
    """
    The class CodeableConcept implements the FHIR CodeableConcept data type.
    This allows to groups different representations of a single concept, e.g.,
    LOINC/123 and SNOMED/456 both represent the eye color feature. Each representation is called a Coding,
    and is composed of a system (LOINC, SNOMED, ...) and a value (123, 456, ...).
    For more details about Codings, see the class Coding.
    """
    def __init__(self, original_name: str):
        """
        Instantiate a new CodeableConcept
        """
        self.coding = []  # coding is a set of Coding - unfortunately FHIR is not consistent with singular/plural...
        self.text = original_name  # the text contains the original column, resp. value, name

    def has_codings(self) -> bool:
        return self.coding is not None and len(self.coding) > 0

    def add_coding(self, one_coding: Coding) -> None:
        if one_coding is not None:
            self.coding.append(one_coding)

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    @classmethod
    def from_json(cls, json_cc: dict):  # returns a CodeableConcept
        log.info(json_cc)
        log.info(json_cc.keys())
        # fill a new CodeableConcept instance with a JSON-encoded CodeableConcept
        cc = CodeableConcept("")
        if "text" in json_cc:
            cc = CodeableConcept(json_cc["text"])
        if "coding" in json_cc:
            cc.coding = []
            log.debug(json_cc["coding"])
            for json_coding in json_cc["coding"]:
                cc.add_coding(one_coding=Coding(ontology=Ontologies.get_enum_from_url(json_coding["system"]), code=json_coding["code"], display=json_coding["display"], column_name=cc.text))
                log.info(cc)
        return cc

    @classmethod
    def create_without_row(cls, ontology1: str, code1: str, ontology2: str, code2: str, column_name: str, column_description: str):
        column_name = normalize_column_name(column_name=column_name)
        cc = CodeableConcept(original_name=column_name)

        ontology1 = normalize_ontology_name(ontology_system=ontology1)
        ontology1 = Ontologies.get_enum_from_name(ontology_name=ontology1)  # get the ontology enum instead of its string name
        code1 = normalize_ontology_code(ontology_code=code1)  # get the ontology code in the metadata for the given column and normalize it (just in case)
        cc.add_coding(one_coding=Coding(ontology=ontology1, code=code1, display=None))

        if ontology2 is not None:
            ontology2 = normalize_ontology_name(ontology_system=ontology2)
            ontology2 = Ontologies.get_enum_from_name(ontology_name=ontology2)
            code2 = normalize_ontology_code(ontology_code=code2)
            cc.add_coding(one_coding=Coding(ontology=ontology2, code=code2, display=None))
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

