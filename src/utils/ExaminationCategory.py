from enum import Enum

from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from utils.Ontologies import Ontologies


class ExaminationCategory(Enum):
    CATEGORY_PHENOTYPIC = Coding(system=Ontologies.LOINC.value["url"], code="81259-4", display="Associated phenotype")
    CATEGORY_CLINICAL = Coding(system=Ontologies.LOINC.value["url"], code="75321-0", display="Clinical finding")

    @classmethod
    def get_phenotypic(cls) -> CodeableConcept:
        cc = CodeableConcept()
        cc.add_coding(coding=ExaminationCategory.CATEGORY_PHENOTYPIC.value)
        return cc

    @classmethod
    def get_clinical(cls) -> CodeableConcept:
        cc = CodeableConcept()
        cc.add_coding(coding=ExaminationCategory.CATEGORY_CLINICAL.value)
        return cc
