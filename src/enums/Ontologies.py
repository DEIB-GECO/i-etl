from typing import Any

import numpy as np
import pandas as pd

from enums.EnumAsClass import EnumAsClass
from utils.setup_logger import log

from utils.str_utils import process_spaces


class Ontologies(EnumAsClass):
    # ontology names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    SNOMEDCT = {"name": "snomedct", "url": "http://snomed.info/sct"}
    LOINC = {"name": "loinc", "url": "http://loinc.org"}
    CLIR = {"name": "clir", "url": "https://clir.mayo.edu"}
    PUBCHEM = {"name": "pubchem", "url": "https://pubchem.ncbi.nlm.nih.gov"}
    GSSO = {"name": "gsso", "url": "http://purl.obolibrary.org/obo"}
    ORPHANET = {"name": "orpha", "url": "https://www.orpha.net/"}
    GENE_ONTOLOGY = {"name": "geneontology", "url": "https://amigo.geneontology.org/amigo"}
    OMIM = {"name": "omim", "url": "https://omim.org"}
    HGNC = {"name": "hgnc", "url": "https://rest.ensembl.org/"}
    NONE = { "name": "none", "url": "none"}

    @classmethod
    def get_enum_from_name(cls, ontology_name: str) -> dict:
        if ontology_name == "":
            return {}
        else:
            for existing_ontology in Ontologies.values():
                if existing_ontology["name"] == ontology_name:
                    return existing_ontology  # return the ontology enum
            return {}

    @classmethod
    def get_enum_from_url(cls, ontology_url: str):
        for existing_ontology in Ontologies.values():
            if existing_ontology["url"] == ontology_url:
                return existing_ontology  # return the ontology enum
        return ""

    @classmethod
    def get_names(cls):
        return [ontology["name"] for ontology in Ontologies.values()]

    @classmethod
    def get_urls(cls):
        return [ontology["url"] for ontology in Ontologies.values()]

    @classmethod
    def normalize_name(cls, ontology_name: str) -> str:
        if ontology_name == "":
            return ""
        else:
            return process_spaces(input_string=ontology_name).lower().replace(" ", "").replace("_", "")

    @classmethod
    def normalize_code(cls, ontology_code: str) -> str:
        if ontology_code == "":
            return ""
        else:
            return process_spaces(input_string=ontology_code).lower().replace(" ", "")
