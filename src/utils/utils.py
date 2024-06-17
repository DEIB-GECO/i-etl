import math
import re
from datetime import time
from typing import Any
import time

from dateutil.parser import parse
from pandas import DataFrame

from src.datatypes.CodeableConcept import CodeableConcept
from src.utils.Ontologies import Ontologies


# ASSERTIONS

def is_float(value) -> bool:
    if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
        try:
            float(value)
            return True
        except ValueError:
            return False
    else:
        # for dict, lists, etc
        return False


def is_not_nan(value) -> bool:
    return not is_float(value) or (is_float(value) and not math.isnan(float(value)))


def assert_not_empty(variable, not_empty=True) -> None:
    if is_not_empty(variable=variable, not_empty=not_empty):
        pass
    else:
        message = "A variable " + variable + " is not supposed to be empty"
        assert False, message


def is_not_empty(variable, not_empty=True) -> bool:
    if isinstance(variable, int) or isinstance(variable, float):
        return True
    elif isinstance(variable, str):
        return variable != ""
    elif isinstance(variable, list):
        if not_empty:
            return variable is not None and variable != []
        else:
            return variable is not None
    elif isinstance(variable, dict):
        if not_empty:
            return variable is not None and variable != {}
        else:
            return variable is not None
    elif isinstance(variable, tuple):
        if not_empty:
            return variable is not None and variable != ()
        else:
            return variable is not None
    elif isinstance(variable, DataFrame):
        return not variable.empty
    elif isinstance(variable, set):
        return variable != set()
    else:
        # no clue about the variable typ
        # thus, we only check whether it is None
        return variable is not None


def is_in_insensitive(value, list_of_compared):
    if not isinstance(value, str):
        return value in list_of_compared
    else:
        for compared in list_of_compared:
            if value.casefold() == compared.casefold():
                return True
        return False


def is_equal_insensitive(value, compared):
    if not isinstance(value, str):
        return value == compared
    else:
        return value.casefold() == compared.casefold()


# GET PREDEFINED VALUES


def get_ontology_system(ontology: str) -> str:
    ontology = normalize_value(ontology)

    for existing_ontology in Ontologies:
        if existing_ontology.value["name"] == ontology:
            return existing_ontology.value["url"]  # return the ontology URI associated to that ontology
    # at the end of the loop, no enum value could match the given ontology
    # thus we need to raise an error
    raise ValueError("The given ontology system '%s' is not known.", ontology)


def get_ontology_resource_uri(ontology_system: str, resource_code: str) -> str:
    return ontology_system + "/" + resource_code


def get_codeable_concept_from_json(codeable_concept_as_json: dict):
    cc = CodeableConcept()
    cc.text = codeable_concept_as_json["text"]
    for coding_as_json in codeable_concept_as_json["coding"]:
        cc.add_coding((coding_as_json["system"], coding_as_json["code"], codeable_concept_as_json["display"]))
    return cc


def get_category_from_json(category_as_json: dict):
    # the category is either PHENOTYPIC or CLINICAL, thus no loop for the codings is necessary
    return CodeableConcept(one_coding=(category_as_json["system"], category_as_json["code"], category_as_json["display"]))


# NORMALIZE DATA

def get_int_from_str(str_value) -> int:
    try:
        return int(str_value)
    except ValueError:
        pass  # this was not an int value


def get_float_from_str(str_value) -> float:
    try:
        return float(str_value)
    except ValueError:
        pass  # this was not a float value


def get_datetime_from_str(str_value) -> str:
    try:
        datetime_value = parse(str_value)
        # %Y-%m-%d %H:%M:%S is the format used by default by parse (the output is always of this form)
        if ":" in str_value:
            # there was a time in the value, let's return a datetime value
            return str(datetime_value)
        else:
            # the value was only a date, so we return only a date too
            return str(datetime_value.date())
    except ValueError:
        pass  # this was not a datetime value


def cast_value(value):
    if isinstance(value, str):
        # trying to cast to something if possible
        # first, try to cast as int
        int_value = get_int_from_str(value)
        if int_value is not None:
            return int_value

        # try to cast as float
        float_value = get_float_from_str(value)
        if float_value is not None:
            return float_value

        # try to cast as datetime (first, because it is more restrictive than simple date)
        datetime_value = get_datetime_from_str(value)
        if datetime_value is not None:
            return datetime_value
    else:
        # log.info("%s is not a string, so no further cast is possible", value)
        return value


def normalize_value(input_string: str) -> str:
    return input_string.upper().strip().replace(" ", "").replace("_", "")


# MONGODB UTILS

def mongodb_match(field: str, value: Any, is_regex: bool = False) -> dict:
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


def get_values_from_json_values(json_values):
    values = []
    for current_dict in json_values:
        if is_not_nan(current_dict) and is_not_empty(current_dict):
            values.append(current_dict["value"])
    return values


def convert_value(value):
    try:
        return float(value)
    except ValueError:
        try:
            return int(value)
        except ValueError:
            return value


# COMPUTE CONSTANTS
def current_milli_time():
    return int(time.time())
