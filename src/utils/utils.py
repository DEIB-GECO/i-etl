import base64
import json
import locale
import math
import os
import xml.dom.minidom
from datetime import datetime, timedelta
from typing import Any

import inflection
import pandas as pd
import requests
from dateutil.parser import parse
from pandas import DataFrame
from requests import Response

from enums.AccessTypes import AccessTypes
from enums.DataTypes import DataTypes
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


# def normalize_value(input_string: str | float, keep_underscores: bool, replace_spaces: bool) -> str:
    # # replace_spaces = True: spaces are replaces by underscores
    # # replace_spaces = False: spaces are deleted
    # return str(input_string).lower().strip().replace("_", "_" if keep_underscores else "").replace(" ", "_" if replace_spaces else "")

def process_spaces(input_string: str) -> str:
    # this removes trailing spaces and replaces sequences of several spaces by only one
    return ' '.join(input_string.split())


def normalize_ontology_name(ontology_system: str) -> str:
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


def normalize_type(data_type: str) -> str:
    if is_not_nan(data_type):
        data_type = process_spaces(input_string=data_type)
        data_type = data_type.lower()

        if data_type == "int" or data_type == "integer":
            return DataTypes.INTEGER
        elif data_type == "str" or data_type == "string":
            return DataTypes.STRING
        elif data_type == "category" or data_type == "categorical":
            return DataTypes.CATEGORY
        elif data_type == "float" or data_type == "numeric":
            return DataTypes.FLOAT
        elif data_type == "bool" or data_type == "boolean":
            return DataTypes.BOOLEAN
        elif data_type == "image file":
            return DataTypes.IMAGE
        elif data_type == "date":
            return DataTypes.DATE
        elif data_type == "datetime" or data_type == "datetime64":
            return DataTypes.DATETIME
        elif data_type == "regex":
            return DataTypes.REGEX
        else:
            log.error(f"{data_type} is not a recognized data type; will use string")
            return DataTypes.STRING
    else:
        return data_type


def cast_value(value: str | float | bool | datetime) -> str | float | bool | datetime:
    log.info(f"{value} of type {type(value)}")
    if isinstance(value, str):
        # try to convert as boolean
        if normalize_column_value(column_value=value) == "true":
            return True
        elif normalize_column_value(column_value=value) == "false":
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
                field: {
                    # the value (the regex) should not contain the / delimiters as in /^[0-9]+$/,
                    # but only the regex, as in "^[0-9]+$"
                    "$regex": value
                }
            }
        }
    else:
        # this is a match with a "hard-coded" value (in value)
        return {
            "$match": {
                field: value
            }
        }


def mongodb_project_one(field: str, projected_value: str|dict|None) -> dict:
    if type(projected_value) is str:
        # in this case, we case to keep a certain field
        # and choose what should be the value of that field (in case of composed fields)
        return {
            "$project": {
                projected_value: "$"+field
            }
        }
    elif type(projected_value) is dict:
        # in case we give a complex projection, e.g., with $split
        return {
            "$project": projected_value
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
                operator: field  # $avg: $<the field on which the avg is computed>
            }
        }
    }


def mongodb_unwind(field: str) -> dict:
    return {
        "$unwind": f"${field}"
    }


def get_mongodb_date_from_datetime(current_datetime: datetime) -> dict:
    return {"$date": current_datetime.strftime(THE_DATETIME_FORMAT)}


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
                log.debug(f"Dumping {len(resource_list)} in {filename}")
                json.dump([resource.to_json() for resource in resource_list], data_file)
            except Exception:
                raise ValueError(f"Could not dump the {len(resource_list)} JSON resources in the file located at {filename}.")
    else:
        log.warning(f"No data when writing file {filename}.")


def get_json_resource_file(current_working_dir: str, table_name: str, count: int) -> str:
    return os.path.join(current_working_dir, f"{table_name}{str(count)}.json")


def read_tabular_file_as_string(filepath: str) -> pd.DataFrame:
    if filepath.endswith(".csv"):
        return pd.read_csv(filepath, index_col=False, dtype=str, keep_default_na=True)
    elif filepath.endswith(".xls") or filepath.endswith(".xlsx"):
        return pd.read_excel(filepath, index_col=False, dtype=str, keep_default_na=True)
    else:
        raise ValueError(f"The extension of the tabular file {filepath} is not recognised. Accepted extensions are .csv, .xls, and .xlsx.")


def split_list_of_files(joined_filepaths: str, prefix_path: str) -> [str]:
    split_files = joined_filepaths.split(",")
    for i in range(len(split_files)):
        current_file = split_files[i]
        if prefix_path is None:
            if not os.path.isfile(current_file):
                raise FileNotFoundError(f"The specified data file {current_file} does not exist.")
        else:
            prefixed_file = os.path.join(prefix_path, current_file)
            if not os.path.isfile(prefixed_file):
                raise FileNotFoundError(f"The specified data file {prefixed_file} does not exist.")
            elif os.sep in current_file:
                raise ValueError(f"The given file ({current_file}) should be a file name, but it looks like a path.")
            else:
                split_files[i] = prefixed_file

    return split_files  # [file 1, file 2, ..., file N]


# ARRAYS
def get_lab_feature_by_text(lab_features: list, lab_feature_text: str) -> dict:
    """
    :param lab_features: list of LabFeature instances
    """
    json_lab_features = [lab_feature.to_json() for lab_feature in lab_features]
    for json_lab_feature in json_lab_features:
        if "code" in json_lab_feature and "text" in json_lab_feature["code"] and json_lab_feature["code"]["text"].startswith(lab_feature_text):
            return json_lab_feature
    return {}


def get_lab_records_for_patient(lab_records: list, patient_id: str) -> list[dict]:
    """
    :param lab_records: list of LabRecord resources
    """
    matching_lab_records = []
    json_lab_records_list = [lab_record.to_json() for lab_record in lab_records]
    log.debug(json_lab_records_list)
    log.debug(patient_id)
    for json_lab_record in json_lab_records_list:
        if json_lab_record["subject"]["reference"] == patient_id:
            matching_lab_records.append(json_lab_record)
    # also sort them by LabFeature reference id
    log.debug(matching_lab_records)
    return sorted(matching_lab_records, key=lambda d: d["instantiate"]["reference"])


def get_field_value_for_patient(lab_records: list, lab_features: list, patient_id: str, column_name: str) -> Any:
    """
    :param lab_records: list of LaboratoryRecord resources
    :param lab_features: list of LaboratoryFeature resource
    :param patient_id: the patient (ID) for which we want to get a specific value
    :param column_name: the column for which we want to get the value
    """

    log.info(f"looking for the value of column {column_name} for patient {patient_id}")

    lab_feature = None
    for lab_feat in lab_features:
        if lab_feat.to_json()["code"]["text"] == column_name:
            lab_feature = lab_feat.to_json()
            break
    if lab_feature is not None:
        log.debug(lab_records)
        log.debug(patient_id)
        for lab_record in lab_records:
            json_lab_record = lab_record.to_json()
            log.info(f"checking {json_lab_record['subject']['reference']} vs. {patient_id} and {json_lab_record['instantiate']['reference']} vs. {lab_feature['identifier']['value']}")
            if json_lab_record["subject"]["reference"] == patient_id:
                if json_lab_record["instantiate"]["reference"] == lab_feature["identifier"]["value"]:
                    return json_lab_record["value"]
    return None


def send_query(url: str, headers: dict | None) -> Response | None:
    try:
        if headers is None:
            return requests.get(url, verify=True)
        else:
            return requests.get(url, headers=headers, verify=True)
    except Exception:
        # if there is an SSL problem, trying the same query without the SSL certificate verification
        try:
            if headers is None:
                return requests.get(url, verify=False)
            else:
                return requests.get(url, headers=headers, verify=False)
        except Exception:
            return None


# API ACCESS
def send_query_to_api(url, secret: str | None, access_type: AccessTypes) -> Response | None:
    # secret may contain an api key or (joint) username and passwort
    if access_type == AccessTypes.USER_AGENT:
        headers = {
            "User-Agent": "Python"
        }
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.AUTHENTICATION:
        username = secret.split(" ")[0]
        password = secret.split(" ")[1]
        base64string = base64.b64encode(bytes('%s:%s' % (username, password), "ascii"))
        headers = {
            "Authorization": f"Basic {base64string.decode("utf-8")}"  # Make sure to prepend 'Bearer ' before your API key
        }
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.API_KEY_IN_HEADER:
        headers = {
            'accept': '"application/json"',
            'apiKey': secret
        }
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.API_KEY_IN_BEARER:
        headers = {
            "Authorization": f"Bearer {secret}"
        }
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.API_KEY_IN_URL:
        url_with_apikey = f"{url}?apikey={secret}"
        return send_query(url=url_with_apikey, headers=None)
    else:
        # unknown access type
        return None


def parse_json_response(response):
    # we need to load x2 and to dump to have a "real" JSON dict, parseable by Python
    # otherwise, we have a JSON-like string or JSNO-like text data
    return json.loads(json.dumps(json.loads(response.content)))


def parse_xml_response(response):
    return xml.dom.minidom.parseString(response.content)


def load_xml_file(filepath: str):
    return xml.dom.minidom.parse(filepath)


def set_env_variables_from_dict(env_vars: dict):
    for key, value in env_vars.items():
        os.environ[key] = value
