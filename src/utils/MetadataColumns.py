from enum import Enum


class MetadataColumns(Enum):
    FIRST_ONTOLOGY_SYSTEM = "ontology"
    FIRST_ONTOLOGY_CODE = "ontology_code"
    FIRST_ONTOLOGY_COMMENT = "ontology_comment"
    SEC_ONTOLOGY_SYSTEM = "secondary_ontology"
    SEC_ONTOLOGY_CODE = "secondary_ontology_code"
    SEC_ONTOLOGY_COMMENT = "secondary_ontology_comment"
    SNOMED_VARTYPE = "snomed_vartype"
    DATASET_NAME = "dataset"
    COLUMN_NAME = "name"
    SIGNIFICATION_IT = "Significato_it"
    SIGNIFICATION_EN = "description"
    VAR_TYPE = "vartype"
    VAR_DIMENSION = "dimension"
    DETAILS = "details"
    JSON_VALUES = "JSON_values"
    MULTIPLICITY = "Multiplicity"
    DOUBTS = "Doubts"
