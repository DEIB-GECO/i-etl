from typing import Any

import inflection

from constants.defaults import NAN_VALUES, DEFAULT_NAN_VALUE
from enums.EnumAsClass import EnumAsClass
from utils.str_utils import process_spaces


class MetadataColumns(EnumAsClass):
    # ontology names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    ONTO_NAME = "ontology"
    ONTO_CODE = "ontology_code"
    DATASET_NAME = "dataset"
    PROFILE = "profile"
    COLUMN_NAME = "name"
    SIGNIFICATION_EN = "description"
    VISIBILITY = "visibility"
    ETL_TYPE = "etl_type"
    VAR_UNIT = "dimension"
    DOMAIN = "domain"
    JSON_VALUES = "json_values"

    @classmethod
    def normalize_name(cls, column_name: str) -> str:
        column_name = process_spaces(input_string=column_name)
        return inflection.underscore(column_name).replace(" ", "_").lower()

    @classmethod
    def normalize_value(cls, column_value: str) -> Any:
        if column_value == "":
            # the cell was empty and kept as is during the data reading
            return ""
        else:
            normalized_value = process_spaces(input_string=str(column_value)).lower()
            if normalized_value in NAN_VALUES:
                # explicit NaN cell value
                return DEFAULT_NAN_VALUE
            else:
                # default case
                return normalized_value
