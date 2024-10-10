from datatypes.OntologyResource import OntologyResource
from enums.EnumAsClass import EnumAsClass
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies


class LabFeatureCategories(EnumAsClass):
    CATEGORY_PHENOTYPIC = OntologyResource(ontology=Ontologies.LOINC, full_code="81259-4", label=None, quality_stats=None)
    CATEGORY_CLINICAL = OntologyResource(ontology=Ontologies.LOINC, full_code="75321-0", label=None, quality_stats=None)
