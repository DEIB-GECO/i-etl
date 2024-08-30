from datatypes.CodeableConcept import CodeableConcept
from enums.EnumAsClass import EnumAsClass
from enums.Ontologies import Ontologies
from utils.utils import normalize_column_name


class PhenotypicColumns(EnumAsClass):
    DOB = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT["name"], code1="184099003", ontology2="loinc", code2="21112-8", column_name="DateOfBirth")
    SEX = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT["name"], code1="734000001", ontology2="loinc", code2="46098-0", column_name="Sex")
    CITY = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC["name"], code1="68997-6", ontology2=None, code2=None, column_name="City")
    GEST_AGE = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC["name"], code1="49051-6", ontology2=None, code2=None, column_name="GestationalAge")
    ETHNICITY_1 = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC["name"], code1="46463-6", ontology2=None, code2=None, column_name="Etnicity")  # for BUZZI in UC1
    ETHNICITY_2 = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC["name"], code1="46463-6", ontology2=None, code2=None, column_name="Ethnicity")  # for tests
    TWINS = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT["name"], code1="28030000", ontology2=None, code2=None, column_name="Twins")
    PREMATURE = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT["name"], code1="206167009", ontology2=None, code2=None, column_name="Premature")
    BIRTH_METHOD = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT["name"], code1="236973005", ontology2=None, code2=None, column_name="BirthMethod")

    @classmethod
    def is_column_phenotypic(cls, column_name: str) -> bool:
        for phenotypic_column in PhenotypicColumns.values():
            if normalize_column_name(column_name) == phenotypic_column.text:
                return True
        return False
