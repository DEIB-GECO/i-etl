from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.OntologyResource import OntologyResource
from enums.EnumAsClass import EnumAsClass
from enums.Ontologies import Ontologies


class TrueFalse(EnumAsClass):
    FALSE = Coding(code=OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="373067005"), display=None)
    TRUE = Coding(code=OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="373066001"), display=None)

    @classmethod
    def get_false(cls) -> CodeableConcept:
        cc = CodeableConcept(original_name="False")
        cc.add_coding(one_coding=TrueFalse.FALSE)
        return cc

    @classmethod
    def get_true(cls) -> CodeableConcept:
        cc = CodeableConcept(original_name="True")
        cc.add_coding(one_coding=TrueFalse.TRUE)
        return cc
