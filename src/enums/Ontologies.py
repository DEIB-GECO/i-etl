from enums.EnumAsClass import EnumAsClass
from utils.assertion_utils import is_not_nan

from utils.str_utils import process_spaces


class Ontologies(EnumAsClass):
    # ontology names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    SNOMEDCT = {"name": "snomedct", "url": "http://snomed.info/sct"}
    LOINC = {"name": "loinc", "url": "http://loinc.org"}
    CLIR = {"name": "clir", "url": "https://clir.mayo.edu"}
    PUBCHEM = {"name": "pubchem", "url": "https://pubchem.ncbi.nlm.nih.gov"}
    GSSO = {"name": "gsso", "url": "http://purl.obolibrary.org/obo"}
    ORPHANET = {"name": "orphanet", "url": "https://www.orpha.net/"}
    GENE_ONTOLOGY = {"name": "geneontology", "url": "https://amigo.geneontology.org/amigo"}

    @classmethod
    def get_enum_from_name(cls, ontology_name: str):
        ontology = cls.normalize_name(ontology_name=ontology_name)

        if not is_not_nan(ontology):
            return None

        for existing_ontology in Ontologies.values():
            if existing_ontology["name"] == ontology:
                return existing_ontology  # return the ontology enum
        return None

    @classmethod
    def get_enum_from_url(cls, ontology_url: str):
        for existing_ontology in Ontologies.values():
            if existing_ontology["url"] == ontology_url:
                return existing_ontology  # return the ontology enum
        return None

    @classmethod
    def get_names(cls):
        return [ontology["name"] for ontology in Ontologies.values()]

    @classmethod
    def get_urls(cls):
        return [ontology["url"] for ontology in Ontologies.values()]

    @classmethod
    def normalize_name(cls, ontology_name: str) -> str:
        if is_not_nan(ontology_name):
            ontology_name = process_spaces(input_string=ontology_name)
            return ontology_name.lower().replace(" ", "").replace("_", "")
        else:
            return ontology_name

    @classmethod
    def normalize_code(cls, ontology_code: str) -> str:
        if is_not_nan(ontology_code):
            ontology_code = process_spaces(input_string=ontology_code)
            return ontology_code.lower().replace(" ", "")
        else:
            return ontology_code

    @classmethod
    def remove_prefix(cls, code: str) -> str:
        if is_not_nan(code):
            return code.replace("ORPHA:", "").replace("orpha:", "").replace("GO:", "").replace("go:", "")
        else:
            return code  # return NaN or None
