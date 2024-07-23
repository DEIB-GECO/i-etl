from enum import Enum

from datatypes.CodeableConcept import CodeableConcept
from enums.Ontologies import Ontologies
from utils.setup_logger import log
from utils.utils import normalize_column_name


class PhenotypicColumns(Enum):
    DOB = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT.value["name"], code1="184099003", ontology2="loinc", code2="21112-8", column_name="DateOfBirth", column_description="Date of birth")
    SEX = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT.value["name"], code1="734000001", ontology2="loinc", code2="46098-0", column_name="Sex", column_description="Sex")
    CITY = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC.value["name"], code1="68997-6", ontology2=None, code2=None, column_name="City", column_description="City of residence")
    GEST_AGE = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC.value["name"], code1="49051-6", ontology2=None, code2=None, column_name="GestationalAge", column_description="Gestational age")
    ETHNICITY_1 = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC.value["name"], code1="46463-6", ontology2=None, code2=None, column_name="Etnicity", column_description="Geographical origin")  # for BUZZI in UC1
    ETHNICITY_2 = CodeableConcept.create_without_row(ontology1=Ontologies.LOINC.value["name"], code1="46463-6", ontology2=None, code2=None, column_name="Ethnicity", column_description="Geographical origin")  # for tests
    TWINS = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT.value["name"], code1="28030000", ontology2=None, code2=None, column_name="Twins", column_description="Twin")
    PREMATURE = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT.value["name"], code1="206167009", ontology2=None, code2=None, column_name="Premature", column_description="Premature baby")
    BIRTH_METHOD = CodeableConcept.create_without_row(ontology1=Ontologies.SNOMEDCT.value["name"], code1="236973005", ontology2=None, code2=None, column_name="BirthMethod", column_description="Birth method")

    @classmethod
    def is_column_phenotypic(cls, column_name: str) -> bool:
        for phenotypic_column in PhenotypicColumns:
            if normalize_column_name(column_name) == phenotypic_column.value.text:
                return True
        return False
