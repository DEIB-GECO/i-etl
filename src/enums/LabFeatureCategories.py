from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.OntologyCode import OntologyCode
from enums.EnumAsClass import EnumAsClass
from enums.Ontologies import Ontologies
from utils.utils import normalize_column_name


class LabFeatureCategories(EnumAsClass):
    CATEGORY_PHENOTYPIC = Coding(ontology=Ontologies.LOINC, code=OntologyCode(full_code="81259-4"), display=None)
    CATEGORY_CLINICAL = Coding(ontology=Ontologies.LOINC, code=OntologyCode(full_code="75321-0"), display=None)

    @classmethod
    def get_phenotypic(cls) -> CodeableConcept:
        cc = CodeableConcept(original_name=normalize_column_name("Phenotypic column"))
        cc.add_coding(one_coding=LabFeatureCategories.CATEGORY_PHENOTYPIC)
        return cc

    @classmethod
    def get_clinical(cls) -> CodeableConcept:
        cc = CodeableConcept(original_name=normalize_column_name("Clinical column"))
        cc.add_coding(one_coding=LabFeatureCategories.CATEGORY_CLINICAL)
        return cc
