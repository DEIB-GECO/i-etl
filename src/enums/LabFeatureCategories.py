from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.OntologyResource import OntologyResource
from enums.EnumAsClass import EnumAsClass
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies


class LabFeatureCategories(EnumAsClass):
    CATEGORY_PHENOTYPIC = Coding(code=OntologyResource(ontology=Ontologies.LOINC, full_code="81259-4"), display=None)
    CATEGORY_CLINICAL = Coding(code=OntologyResource(ontology=Ontologies.LOINC, full_code="75321-0"), display=None)

    @classmethod
    def get_phenotypic(cls) -> CodeableConcept:
        cc = CodeableConcept(original_name=MetadataColumns.normalize_name("Phenotypic column"))
        cc.add_coding(one_coding=LabFeatureCategories.CATEGORY_PHENOTYPIC)
        return cc

    @classmethod
    def get_clinical(cls) -> CodeableConcept:
        cc = CodeableConcept(original_name=MetadataColumns.normalize_name("Clinical column"))
        cc.add_coding(one_coding=LabFeatureCategories.CATEGORY_CLINICAL)
        return cc
