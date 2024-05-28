from enum import Enum

from src.utils.Ontologies import Ontologies


class ExaminationCategory(Enum):
    CATEGORY_CLINICAL = (Ontologies.LOINC.value[1], "81259-4", "Associated phenotype")
    CATEGORY_PHENOTYPIC = (Ontologies.LOINC.value[1], "75321-0", "Clinical finding")
