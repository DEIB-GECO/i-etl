from enums.EnumAsClass import EnumAsClass
from utils.utils import normalize_ontology_name, is_not_nan


class Ontologies(EnumAsClass):

    SNOMEDCT = {"name": normalize_ontology_name("SNOMEDCT"), "url": "http://snomed.info/sct"}
    LOINC = {"name": normalize_ontology_name("LOINC"), "url": "http://loinc.org"}
    CLIR = {"name": normalize_ontology_name("CLIR"), "url": "https://clir.mayo.edu"}
    PUBCHEM = {"name": normalize_ontology_name("PUBCHEM"), "url": "https://pubchem.ncbi.nlm.nih.gov"}
    GSSO = {"name": normalize_ontology_name("GSSO"), "url": "http://purl.obolibrary.org/obo"}
    ORPHANET = {"name": normalize_ontology_name("ORPHANET"), "url": "https://www.orpha.net/"}

    @classmethod
    def get_enum_from_name(cls, ontology_name: str):
        ontology = normalize_ontology_name(ontology_system=ontology_name)

        if not is_not_nan(ontology):
            return None

        for existing_ontology in Ontologies.values():
            if existing_ontology["name"] == ontology:
                return existing_ontology  # return the ontology enum
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError(f"The given ontology name ({ontology}) does not correspond to any known ontology.")

    @classmethod
    def get_enum_from_url(cls, ontology_url: str):
        for existing_ontology in Ontologies.values():
            if existing_ontology["url"] == ontology_url:
                return existing_ontology  # return the ontology enum
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError(f"The given ontology url ({ontology_url}) does not correspond to any known ontology.")

    @classmethod
    def get_url_from_name(cls, ontology_name: str) -> str:
        ontology_name = normalize_ontology_name(ontology_system=ontology_name)

        if not is_not_nan(ontology_name):
            return None

        for existing_ontology in Ontologies.values():
            if existing_ontology["name"] == ontology_name:
                return existing_ontology["url"]  # return the ontology URI associated to that ontology
        # at the end of the loop, no enum value could match the given ontology
        # thus we need to raise an error
        raise ValueError(f"The given ontology system {ontology_name} is not known.")

    @classmethod
    def get_ontology_resource_uri(cls, ontology_system: str, resource_code: str) -> str:
        return f"{ontology_system}/{resource_code}"
