from datatypes.OntologyResource import OntologyResource
from enums.EnumAsClass import EnumAsClass
from enums.Ontologies import Ontologies


class TrueFalse(EnumAsClass):
    FALSE = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="373067005", label=None, quality_stats=None)
    TRUE = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="373066001", label=None, quality_stats=None)
