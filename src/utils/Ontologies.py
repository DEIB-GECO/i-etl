from enum import Enum

from utils.utils import normalize_value


class Ontologies(Enum):

    SNOMEDCT = {"name": "SNOMEDCT", "url": ""}
    LOINC = {"name": "LOINC", "url": "http://loinc.org"}
    CLIR = {"name": "CLIR", "url": "https://clir.mayo.edu/"}
    PUBCHEM = {"name": "PUBCHEM", "url": "https://pubchem.ncbi.nlm.nih.gov/"}
    GSSO = {"name": "GSSO", "url": "http://purl.obolibrary.org/obo/"}

    @classmethod
    def get_ontology_system(cls, ontology: str) -> str:
        ontology = normalize_value(input_string=ontology)

        for existing_ontology in Ontologies:
            if existing_ontology.value["name"] == ontology:
                return existing_ontology.value["url"]  # return the ontology URI associated to that ontology
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError("The given ontology system '%s' is not known.", ontology)

    @classmethod
    def get_ontology_resource_uri(cls, ontology_system: str, resource_code: str) -> str:
        return ontology_system + "/" + resource_code
