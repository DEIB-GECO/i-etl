from utils.utils import normalize_column_name


class MetadataColumns:
    FIRST_ONTOLOGY_SYSTEM = normalize_column_name(column_name="ontology")
    FIRST_ONTOLOGY_CODE = normalize_column_name(column_name="ontology_code")
    FIRST_ONTOLOGY_COMMENT = normalize_column_name(column_name="ontology_comment")
    SEC_ONTOLOGY_SYSTEM = normalize_column_name(column_name="secondary_ontology")
    SEC_ONTOLOGY_CODE = normalize_column_name(column_name="secondary_ontology_code")
    SEC_ONTOLOGY_COMMENT = normalize_column_name(column_name="secondary_ontology_comment")
    SNOMED_VARTYPE = normalize_column_name(column_name="snomed_vartype")
    DATASET_NAME = normalize_column_name(column_name="dataset")
    COLUMN_NAME = normalize_column_name(column_name="name")
    SIGNIFICATION_IT = normalize_column_name(column_name="Significato_it")
    SIGNIFICATION_EN = normalize_column_name(column_name="description")
    VAR_TYPE = normalize_column_name(column_name="vartype")
    VAR_DIMENSION = normalize_column_name(column_name="dimension")
    DETAILS = normalize_column_name(column_name="details")
    JSON_VALUES = normalize_column_name(column_name="JSON_values")
    MULTIPLICITY = normalize_column_name(column_name="Multiplicity")
    DOUBTS = normalize_column_name(column_name="Doubts")

    @classmethod
    def values(cls):
        xs = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                xs.append(value)
        return xs
