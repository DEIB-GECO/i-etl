import math
import re
from datetime import datetime
from typing import Any

from dateutil.parser import parse

from src.utils.setup_logger import log

# CONSTANTS

ONTOLOGY_SNOMED = "http://snomed.info/sct"
ONTOLOGY_LOINC = "http://loinc.org"

CATEGORY_CLINICAL = (ONTOLOGY_LOINC, "81259-4", "Associated phenotype")
CATEGORY_PHENOTYPIC = (ONTOLOGY_LOINC, "75321-0", "Clinical finding")


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


def assert_type(variable, expected_type, variable_name: str) -> None:
    assert isinstance(variable_name, str)

    assert isinstance(variable, expected_type), "The variable " + variable_name + " is of type " + str(
        type(variable)) + " while it should be of type " + str(expected_type) + "."


def assert_not_empty(variable, variable_name: str, check_not_empty=True) -> None:
    assert_type(variable=variable_name, expected_type=str, variable_name=variable_name)

    message = "The variable " + variable_name + " is not supposed to be empty"
    if isinstance(variable, int) or isinstance(variable, float):
        assert True
    elif isinstance(variable, str):
        assert variable != "", message
    elif isinstance(variable, list):
        assert variable is not None, message
        if check_not_empty:
            assert variable != [], message
    elif isinstance(variable, dict):
        assert variable is not None, message
        if check_not_empty:
            assert variable != {}, message
    elif isinstance(variable, tuple):
        assert variable is not None, message
        if check_not_empty:
            assert variable != (), message
    else:
        # no clue about the variable typ
        # thus, we only check whether it is None
        assert variable is not None, message


def assert_variable(variable, expected_type, variable_name: str, check_not_empty=True) -> None:
    assert_type(variable=variable, expected_type=expected_type, variable_name=variable_name)
    assert_not_empty(variable=variable, variable_name=variable_name, check_not_empty=check_not_empty)


def assert_regex(value: str, regex: str) -> None:
    assert_type(variable=value, expected_type=str, variable_name="value")
    assert_type(variable=regex, expected_type=str, variable_name="regex")

    # returns True if the string matches the given regex, False else
    assert bool(re.match(pattern=regex, string=value)) is True, "The value " + value + "does not match the regex '" + regex + "'."

# BUILDING URLs


def build_url(base: str, id: int) -> str:
    assert_type(base, str, "base")
    assert_type(id, int, "id")

    return base + "/" + str(id)

# GET PREDEFINED VALUES


def get_ontology_system(ontology: str) -> str:
    ontology = ontology.strip()
    ontology = ontology.replace(" ", "_")
    ontology = ontology.upper()
    if ontology == "SNOMED_CT":
        return ONTOLOGY_SNOMED
    elif ontology == "LOINC":
        return ONTOLOGY_LOINC
    else:
        raise ValueError("The given ontology system '%S' is not known.", ontology)


def get_ontology_code(ontology_code: str) -> str:
    ontology_code = ontology_code.strip()  # remove spaces around : for compound SNOMED_CT codes

    return ontology_code


def normalize_value(value):
    if isinstance(value, str):
        # trying to cast to something if possible
        # try to cast as float
        try:
            float_value = float(value)
            log.debug("Will return %s as a float", float_value)
            return float_value
        except ValueError:
            pass  # this was not a float value
        # try to cast as datetime (first, because it is more restrictive than simple date)
        try:
            datetime_value = parse(value)
            # %Y-%m-%d %H:%M:%S is the format used by default by parse (the output is always of this form)
            if ":" in value:
                # there was a time in the value, let's return a datetime value
                return str(datetime_value)
            else:
                # the value was only a date, so we return only a date too
                return str(datetime_value.date())
        except ValueError:
            pass  # this was not a datetime value
        return value
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
