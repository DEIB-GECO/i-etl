from enum import Enum

from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from enums.Ontologies import Ontologies


class LabFeatureCategory(Enum):
    CATEGORY_PHENOTYPIC = Coding(system=Ontologies.LOINC.value["url"], code="81259-4", name="Phenotypic feature", description="Associated phenotype")
    CATEGORY_CLINICAL = Coding(system=Ontologies.LOINC.value["url"], code="75321-0", name="Clinical feature", description="Clinical finding")

    @classmethod
    def get_phenotypic(cls) -> CodeableConcept:
        cc = CodeableConcept()
        cc.add_coding(one_coding=LabFeatureCategory.CATEGORY_PHENOTYPIC.value)
        return cc

    @classmethod
    def get_clinical(cls) -> CodeableConcept:
        cc = CodeableConcept()
        cc.add_coding(one_coding=LabFeatureCategory.CATEGORY_CLINICAL.value)
        return cc
