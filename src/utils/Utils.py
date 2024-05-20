import math
import re
from datetime import datetime
from typing import Any

from dateutil.parser import parse
from pandas import DataFrame

from src.utils.setup_logger import log

# CONSTANTS

ONTOLOGY_SNOMED_NAME = "snomed_ct"
ONTOLOGY_SNOMED_URL = "http://snomed.info/sct"
ONTOLOGY_LOINC_NAME = "loinc"
ONTOLOGY_LOINC_URL = "http://loinc.org"

CATEGORY_CLINICAL = (ONTOLOGY_LOINC_URL, "81259-4", "Associated phenotype")
CATEGORY_PHENOTYPIC = (ONTOLOGY_LOINC_URL, "75321-0", "Clinical finding")


HOSPITAL_TABLE_NAME = "Hospital"
PATIENT_TABLE_NAME = "Patient"
EXAMINATION_TABLE_NAME = "Examination"
EXAMINATION_RECORD_TABLE_NAME = "ExaminationRecord"
DISEASE_RECORD_TABLE_NAME = "DiseaseRecord"
DISEASE_TABLE_NAME = "Disease"



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


def assert_type(variable, expected_type) -> None:
    message = "The variable '" + str(variable) + (" "
                "is of type ") + str(type(variable)) + (" "
                "while it should be of type ") + str(expected_type) + "."
    assert isinstance(variable, expected_type), message


def assert_not_empty(variable, not_empty=True) -> None:
    if is_not_empty(variable=variable, not_empty=not_empty):
        assert True
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


def assert_variable(variable, expected_type, not_empty=True) -> None:
    assert_type(variable=variable, expected_type=expected_type)
    assert_not_empty(variable=variable, not_empty=not_empty)


def assert_regex(value: str, regex: str) -> None:
    assert_type(variable=value, expected_type=str)
    assert_type(variable=regex, expected_type=str)

    # returns True if the string matches the given regex, False else
    assert bool(re.match(pattern=regex, string=value)) is True, "'" + value + "' does not match '" + regex + "'."


def is_equal_insensitive(value, compared):
    if not isinstance(value, str):
        return value == compared
    else:
        return value.casefold() == compared.casefold()


# BUILDING URLs


def build_url(base: str, element_id: int) -> str:
    assert_type(base, str)
    assert_type(element_id, int)

    return base + "/" + str(id)


# GET PREDEFINED VALUES


def get_ontology_system(ontology: str) -> str:
    ontology = ontology.strip()
    ontology = ontology.replace(" ", "_")
    ontology = ontology.upper()
    if ontology == "SNOMED_CT":
        return ONTOLOGY_SNOMED_URL
    elif ontology == "LOINC":
        return ONTOLOGY_LOINC_URL
    else:
        raise ValueError("The given ontology system '%S' is not known.", ontology)


def get_ontology_code(ontology_code: str) -> str:
    ontology_code = ontology_code.strip()  # remove spaces around : for compound SNOMED_CT codes

    return ontology_code


def get_int_from_str(str_value) -> int:
    try:
        return int(str_value)
    except ValueError:
        return None  # this was not an int value


def get_float_from_str(str_value) -> float:
    try:
        return float(str_value)
    except ValueError:
        return None  # this was not a float value


def get_datetime_from_str(str_value):
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
    return None


def normalize_value(value):
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


# MONGODB UTILS

def mongodb_match(field: str, value: Any) -> dict:
    return {
        "$match": {
            field: value
        }
    }


def mongodb_project_one(field: str) -> dict:
    return {
        "$project": {
            field: 1
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
                operator: "$"+field  # $avg: $<the field on which the avg is computed>
            }
        }
    }


# LIST AND DICT CONVERSIONS


def get_values_from_JSON_values(json_values):
    values = []
    for current_dict in json_values:
        if is_not_nan(current_dict) and is_not_empty(current_dict):
            values.append(current_dict["value"])
    return values


def convert_value(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value
