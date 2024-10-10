from typing import Any

import inflection

from enums.EnumAsClass import EnumAsClass
from utils.assertion_utils import is_not_nan
from utils.str_utils import process_spaces


class MetadataColumns(EnumAsClass):
    # ontology names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    ONTO_NAME = "ontology"
    ONTO_CODE = "ontology_code"
    DATASET_NAME = "dataset"
    COLUMN_NAME = "name"
    SIGNIFICATION_EN = "description"
    PHENOTYPIC = "phenotypic"
    VISIBILITY = "visibility"
    VAR_TYPE = "vartype"
    ETL_TYPE = "etl_type"
    VAR_DIMENSION = "dimension"
    JSON_VALUES = "json_values"

    @classmethod
    def normalize_name(cls, column_name: str) -> str:
        if is_not_nan(column_name):
            column_name = process_spaces(input_string=column_name)
            return inflection.underscore(column_name).replace(" ", "_").lower()
        else:
            return column_name

    @classmethod
    def normalize_value(cls, column_value: Any) -> str:
        if is_not_nan(column_value):
            column_value = str(column_value)
            column_value = process_spaces(input_string=column_value)
            return column_value.lower()
        else:
            return column_value
