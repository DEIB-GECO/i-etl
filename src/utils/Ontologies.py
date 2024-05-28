from enum import Enum


class Ontologies(Enum):
    SNOMEDCT = ("SNOMEDCT", "http://snomed.info/sct")
    LOINC = ("LOINC", "http://loinc.org")
    CLIR = ("CLIR", "https://clir.mayo.edu/")
    PUBCHEM = ("PUBCHEM", "https://pubchem.ncbi.nlm.nih.gov/")
