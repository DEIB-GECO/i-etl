from enum import Enum

from utils.utils import normalize_ontology_system, is_not_nan


class Ontologies(Enum):

    SNOMEDCT = {"name": normalize_ontology_system("SNOMEDCT"), "url": "http://snomed.info/sct/"}
    LOINC = {"name": normalize_ontology_system("LOINC"), "url": "http://loinc.org"}
    CLIR = {"name": normalize_ontology_system("CLIR"), "url": "https://clir.mayo.edu/"}
    PUBCHEM = {"name": normalize_ontology_system("PUBCHEM"), "url": "https://pubchem.ncbi.nlm.nih.gov/"}
    GSSO = {"name": normalize_ontology_system("GSSO"), "url": "http://purl.obolibrary.org/obo/"}

    @classmethod
    def get_ontology_system(cls, ontology: str) -> str:
        ontology = normalize_ontology_system(ontology_system=ontology)

        if not is_not_nan(ontology):
            return None

        for existing_ontology in Ontologies:
            if existing_ontology.value["name"] == ontology:
                return existing_ontology.value["url"]  # return the ontology URI associated to that ontology
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError(f"The given ontology system {ontology} is not known.")

    @classmethod
    def get_ontology_resource_uri(cls, ontology_system: str, resource_code: str) -> str:
        return ontology_system + "/" + resource_code
