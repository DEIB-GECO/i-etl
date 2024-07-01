from enum import Enum


class Ontologies(Enum):
    SNOMEDCT = { "name": "SNOMEDCT", "url": "" }
    LOINC = { "name": "LOINC", "url":  "http://loinc.org" }
    CLIR = { "name": "CLIR", "url": "https://clir.mayo.edu/" }
    PUBCHEM = { "name": "PUBCHEM", "url": "https://pubchem.ncbi.nlm.nih.gov/" }
    GSSO = { "name": "GSSO", "url": "http://purl.obolibrary.org/obo/" }
