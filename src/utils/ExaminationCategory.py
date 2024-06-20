from enum import Enum

from src.utils.Ontologies import Ontologies


class ExaminationCategory(Enum):
    CATEGORY_PHENOTYPIC = (Ontologies.LOINC.value["url"], "81259-4", "Associated phenotype")
    CATEGORY_CLINICAL = (Ontologies.LOINC.value["url"], "75321-0", "Clinical finding")
