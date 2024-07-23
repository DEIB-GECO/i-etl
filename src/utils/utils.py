import json
import locale
import math
import os
import re
from datetime import datetime, timedelta
from typing import Any

import inflection
import pandas as pd
from dateutil.parser import parse
from pandas import DataFrame


from utils.setup_logger import log

# moving it to constants.py creates a circular dependency; also it is only used in this file
THE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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
    return (not is_float(value=value) and value is not None) or (is_float(value=value) and not math.isnan(float(value)))


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


def is_equal_insensitive(value: str | float, compared: str | float) -> bool:
    if not isinstance(value, str):
        return value == compared
    else:
        return value.casefold() == compared.casefold()


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
    return {"$date": current_datetime.strftime(THE_DATETIME_FORMAT)}


# def normalize_value(input_string: str | float, keep_underscores: bool, replace_spaces: bool) -> str:
    # # replace_spaces = True: spaces are replaces by underscores
    # # replace_spaces = False: spaces are deleted
    # return str(input_string).lower().strip().replace("_", "_" if keep_underscores else "").replace(" ", "_" if replace_spaces else "")

def process_spaces(input_string: str) -> str:
    # this removes trailing spaces and replaces sequences of several spaces by only one
    return ' '.join(input_string.split())


def normalize_ontology_system(ontology_system: str) -> str:
    if is_not_nan(ontology_system):
        ontology_system = process_spaces(input_string=ontology_system)
        return ontology_system.lower().replace(" ", "").replace("_", "")
    else:
        return ontology_system


def normalize_ontology_code(ontology_code: str) -> str:
    if is_not_nan(ontology_code):
        ontology_code = process_spaces(input_string=ontology_code)
        return ontology_code.lower().replace(" ", "")
    else:
        return ontology_code


def normalize_column_name(column_name: str) -> str:
    if is_not_nan(column_name):
        column_name = process_spaces(input_string=column_name)
        return inflection.underscore(column_name).replace(" ", "_").lower()
    else:
        return column_name


def normalize_column_value(column_value: Any) -> str:
    if is_not_nan(column_value):
        column_value = str(column_value)
        column_value = process_spaces(input_string=column_value)
        return column_value.lower()
    else:
        return column_value


def normalize_hospital_name(hospital_name: str) -> str:
    if is_not_nan(hospital_name):
        hospital_name = process_spaces(hospital_name)
        hospital_name = hospital_name.replace(" ", "_")  # add missing _ in hospital name if needed
        return inflection.underscore(hospital_name).lower()
    else:
        return hospital_name


def normalize_var_type(var_type: str) -> str:
    if is_not_nan(var_type):
        var_type = process_spaces(input_string=var_type)
        return var_type.lower()
    else:
        return var_type


def cast_value(value: str | float | bool | datetime) -> str | float | bool | datetime:
    if isinstance(value, str):
        # try to convert as boolean
        if value == "True" or value == "true":
            return True
        elif value == "False" or value == "false":
            return False

        # try to cast as float
        float_value = get_float_from_str(str_value=value)
        if float_value is not None:
            return float_value

        # try to cast as date
        log.debug(f"trying to cast value {value}")
        datetime_value = get_datetime_from_str(str_value=value)
        log.debug(f"obtained datetime value: {value}")
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


def get_display(name: str, description: str) -> str:
    display = name  # row[MetadataColumns.COLUMN_NAME]
    if is_not_nan(description):  # row[MetadataColumns.SIGNIFICATION_EN]):
        # by default the display is the variable name
        # if we also have a description, we append it to the display
        # e.g., "BTD (human biotinidase activity)"
        display = f"{display} ({str(description)})"
    return display


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


def mongodb_project_one(field: str, projected_value: str|None) -> dict:
    if type(projected_value) is str:
        # in this case, we case to keep a certain field
        # and choose what should be the value of that field (in case of composed fields)
        return {
            "$project": {
                projected_value: "$"+field
            }
        }
    else:
        # in that case, we only want to keep a certain field
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
                operator: "$" + field  # $avg: $<the field on which the avg is computed>
            }
        }
    }


def mongodb_unwind(field: str) -> dict:
    return {
        "$unwind": "$" + field
    }


# LIST AND DICT CONVERSIONS


def get_values_from_json_values(json_values: dict) -> list[dict]:
    values = []
    for current_dict in json_values:
        if is_not_nan(value=current_dict) and is_not_empty(variable=current_dict):
            values.append(current_dict["value"])
    return values


# TESTS

# for comparing keys in original and inserted objects
def compare_keys(original_object: dict, inserted_object: dict) -> bool:
    original_keys = list(original_object.keys())
    inserted_keys = list(inserted_object.keys())
    if "_id" in inserted_keys:
        inserted_keys.remove("_id")
    # we need to sort the keys, otherwise the equality would not work, e.g., ["a", "b"] != ["b", "a"]
    inserted_keys.sort()
    original_keys.sort()

    return inserted_keys == original_keys


# for comparing two tuples
def compare_tuples(original_tuple: dict, inserted_tuple: dict) -> None:
    assert inserted_tuple is not None, "The inserted tuple is None."
    assert compare_keys(original_object=original_tuple, inserted_object=inserted_tuple) is True, different_keys()
    for original_key in original_tuple:
        assert original_key in inserted_tuple, missing_attribute(original_key)
        if original_key == "timestamp":
            # for the special case of timestamp attributes, we check +/- few milliseconds
            # to avoid failing when conversions do not yield the exact same datetime value
            inserted_time = datetime.strptime(inserted_tuple[original_key]["$date"], THE_DATETIME_FORMAT)
            log.debug(inserted_time)
            inserted_time_minus = inserted_time - timedelta(milliseconds=100)
            inserted_time_plus = inserted_time + timedelta(milliseconds=100)
            assert inserted_time_minus <= inserted_time <= inserted_time_plus
        else:
            assert original_tuple[original_key] == inserted_tuple[original_key], different_values(original_key)


# for logging when assert fails in test
def missing_attribute(attribute: str) -> str:
    return f"The inserted tuple is missing the attribute '{attribute}'."


def different_values(attribute: str) -> str:
    return f"The value for '{attribute}' differs between the original and the inserted tuples."


def different_keys() -> str:
    return "The keys of the original and inserted tuple differ."


def wrong_number_of_docs(number: int):
    return f"The expected number of documents is {number}."


# READ/WRITE IN FILES
def write_in_file(resource_list: list, current_working_dir: str, table_name: str, count: int) -> None:
    filename = get_json_resource_file(current_working_dir=current_working_dir, table_name=table_name, count=count)
    if len(resource_list) > 0:
        with open(filename, "w") as data_file:
            try:
                log.debug(f"Writing resource list: {resource_list}")
                json.dump([resource.to_json() for resource in resource_list], data_file)
            except Exception:
                raise ValueError(f"Could not dump the {len(resource_list)} JSON resources in the file located at {filename}.")
    else:
        log.warning(f"No data when writing file {filename}.")


def get_json_resource_file(current_working_dir: str, table_name: str, count: int) -> str:
    return os.path.join(current_working_dir, table_name + str(count) + ".json")


def read_csv_file_as_string(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath, index_col=False, dtype=str, keep_default_na=True)


# ARRAYS
def get_examination_by_text_in_list(examinations_list: list, examination_text: str) -> dict:
    """
    :param examinations_list: list of Examination resources
    """
    json_examinations_list = [examination.to_json() for examination in examinations_list]
    log.debug(json_examinations_list)
    log.debug(examination_text)
    for json_examination in json_examinations_list:
        if "code" in json_examination and "text" in json_examination["code"] and json_examination["code"]["text"].startswith(examination_text):
            return json_examination
    return {}


def get_examination_records_by_patient_id_in_list(examination_records_list: list, patient_id: str) -> list[dict]:
    """
    :param examination_records_list: list of ExaminationRecord resources
    """
    matching_examination_records = []
    json_examination_records_list = [examination_record.to_json() for examination_record in examination_records_list]
    log.debug(json_examination_records_list)
    log.debug(patient_id)
    for json_examination_record in json_examination_records_list:
        if json_examination_record["subject"]["reference"] == patient_id:
            matching_examination_records.append(json_examination_record)
    # also sort them by Examination reference id
    log.debug(matching_examination_records)
    return sorted(matching_examination_records, key=lambda d: d["instantiate"]["reference"])
