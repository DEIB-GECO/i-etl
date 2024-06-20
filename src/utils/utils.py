import locale
import math
import re
from datetime import datetime
from typing import Any

from dateutil.parser import parse
from pandas import DataFrame

from src.utils.Ontologies import Ontologies


# ASSERTIONS

def is_float(value: Any) -> bool:
    if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
        try:
            float(value)
            return True
        except ValueError:
            return False
    else:
        # for dict, lists, etc
        return False


def is_not_nan(value: Any) -> bool:
    return not is_float(value=value) or (is_float(value=value) and not math.isnan(float(value)))


def is_not_empty(variable: Any) -> bool:
    if isinstance(variable, int) or isinstance(variable, float):
        return True
    elif isinstance(variable, str):
        return variable != ""
    elif isinstance(variable, list):
        return variable is not None and variable != []
    elif isinstance(variable, dict):
        return variable is not None and variable != {}
    elif isinstance(variable, tuple):
        return variable is not None and variable != ()
    elif isinstance(variable, DataFrame):
        return not variable.empty
    elif isinstance(variable, set):
        return variable != set()
    else:
        # no clue about the variable typ
        # thus, we only check whether it is None
        return variable is not None


def is_in_insensitive(value: Any, list_of_compared: list[Any]) -> bool:
    if not isinstance(value, str):
        return value in list_of_compared
    else:
        for compared in list_of_compared:
            if value.casefold() == compared.casefold():
                return True
        return False


def is_equal_insensitive(value: str | float, compared: str | float) -> bool:
    if not isinstance(value, str):
        return value == compared
    else:
        return value.casefold() == compared.casefold()


# GET PREDEFINED VALUES


def get_ontology_system(ontology: str) -> str:
    ontology = normalize_value(input_string=ontology)

    for existing_ontology in Ontologies:
        if existing_ontology.value["name"] == ontology:
            return existing_ontology.value["url"]  # return the ontology URI associated to that ontology
    # at the end of the loop, no enum value could match the given ontology
    # thus we need to raise an error
    raise ValueError("The given ontology system '%s' is not known.", ontology)


def get_ontology_resource_uri(ontology_system: str, resource_code: str) -> str:
    return ontology_system + "/" + resource_code


# NORMALIZE DATA

def get_int_from_str(str_value: str):
    try:
        return int(str_value)
    except ValueError:
        return None  # this was not an int value


def get_float_from_str(str_value: str):
    try:
        return locale.atof(str_value)
    except ValueError:
        return None  # this was not a float value


def get_datetime_from_str(str_value: str) -> datetime:
    try:
        datetime_value = parse(str_value)
        # %Y-%m-%d %H:%M:%S is the format used by default by parse (the output is always of this form)
        return datetime_value
    except ValueError:
        # this was not a datetime value, and we signal it with None
        return None


def get_mongodb_date_from_datetime(current_datetime: datetime) -> dict:
    return { "$date": current_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') }


def normalize_value(input_string: str) -> str:
    return input_string.upper().strip().replace(" ", "").replace("_", "")


def convert_value(value: str | float | bool | datetime) -> str | float | bool | datetime:
    if isinstance(value, str):
        # try to convert as boolean
        if value == "True":
            return True
        elif value == "False":
            return False

        # try to cast as float
        float_value = get_float_from_str(str_value=value)
        if float_value is not None:
            return float_value

        # try to cast as date
        datetime_value = get_datetime_from_str(str_value=value)
        if datetime_value is not None:
            return datetime_value

        # finally, try to cast as integer
        int_value = get_int_from_str(str_value=value)
        if int_value is not None:
            return int_value

        # no cast could be applied, we return the value as is
        return value
    else:
        # this is already cast to the right type, nothing more to do
        return value


# MONGODB UTILS

def mongodb_match(field: str, value: Any, is_regex: bool) -> dict:
    if is_regex:
        # this is a match with a regex (in value)
        return {
            "$match": {
                field: re.compile(value)
            }
        }
    else:
        # this is a match with a "hard-coded" value (in value)
        return {
            "$match": {
                field: value
            }
        }


def mongodb_project_one(field: str, split_delimiter: str) -> dict:
    if split_delimiter is None:
        return {
            "$project": {
                field: 1
            }
        }
    else:
        # also split the projected value
        return {
            "$project": {
                field: {
                    # we prepend a $ to the fild so that MongoDB understand that
                    # it needs to parse the actual value of the filed
                    "$split": ["$"+field, split_delimiter]
                }
            }
        }


def mongodb_sort(field: str, sort_order: int) -> dict:
    return {
        "$sort": {
            field: sort_order
        }
    }


def mongodb_limit(nb: int) -> dict:
    return {
        "$limit": nb
    }


def mongodb_group_by(group_key: Any, group_by_name: str, operator: str, field) -> dict:
    return {
        "$group": {
            "_id": group_key,
            group_by_name: {
                operator: "$" + field  # $avg: $<the field on which the avg is computed>
            }
        }
    }


def mongodb_unwind(field: str) -> dict:
    return {
        "$unwind": "$" + field
    }


def mongodb_max(field: str) -> dict:
    return {
        "$group": {
            "_id": field,
            "max": {
                "$max": {
                    "$toLong": "$"+field  # to make the field interpreted
                }
            }
        }
    }


def mongodb_min(field: str) -> dict:
    return {
        "$group": {
            "_id": field,
            "min": {
                "$min": {
                    "$toLong": "$"+field  # to make the field interpreted
                }
            }
        }
    }

# LIST AND DICT CONVERSIONS


def get_values_from_json_values(json_values: dict) -> list[dict]:
    values = []
    for current_dict in json_values:
        if is_not_nan(value=current_dict) and is_not_empty(variable=current_dict):
            values.append(current_dict["value"])
    return values
