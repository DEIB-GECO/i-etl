from enums.EnumAsClass import EnumAsClass
from utils.utils import normalize_column_name


class MetadataColumns(EnumAsClass):
    FIRST_ONTOLOGY_NAME = normalize_column_name(column_name="ontology")
    FIRST_ONTOLOGY_CODE = normalize_column_name(column_name="ontology_code")
    SEC_ONTOLOGY_NAME = normalize_column_name(column_name="secondary_ontology")
    SEC_ONTOLOGY_CODE = normalize_column_name(column_name="secondary_ontology_code")
    DATASET_NAME = normalize_column_name(column_name="dataset")
    COLUMN_NAME = normalize_column_name(column_name="name")
    SIGNIFICATION_EN = normalize_column_name(column_name="description")
    VAR_TYPE = normalize_column_name(column_name="vartype")
    ETL_TYPE = normalize_column_name(column_name="ETL_type")
    VAR_DIMENSION = normalize_column_name(column_name="dimension")
    JSON_VALUES = normalize_column_name(column_name="JSON_values")
