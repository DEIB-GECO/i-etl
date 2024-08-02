from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from enums.EnumAsClass import EnumAsClass
from enums.Ontologies import Ontologies


class TrueFalse(EnumAsClass):
    FALSE = Coding(system=Ontologies.SNOMEDCT["url"], code="373067005", name="False", description="False boolean value")
    TRUE = Coding(system=Ontologies.SNOMEDCT["url"], code="373066001", name="True", description="True boolean value")

    @classmethod
    def get_false(cls) -> CodeableConcept:
        cc = CodeableConcept()
        cc.add_coding(one_coding=TrueFalse.FALSE)
        return cc

    @classmethod
    def get_true(cls) -> CodeableConcept:
        cc = CodeableConcept()
        cc.add_coding(one_coding=TrueFalse.TRUE)
        return cc
