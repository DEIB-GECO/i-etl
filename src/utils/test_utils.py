import os
from datetime import timedelta, datetime
from typing import Any

from database.Operators import THE_DATETIME_FORMAT
from entities.Record import Record
from entities.Resource import Resource
from utils.setup_logger import log


# for setting up the tests with specific (env) parameters
def set_env_variables_from_dict(env_vars: dict):
    # it is not possible to change the values of a .env and to write them in the .env file
    # so, we simply change them within the env
    for key, value in env_vars.items():
        os.environ[key] = value


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
        if original_key == Resource.TIMESTAMP_:
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


def get_feature_by_text(features: list, feature_text: str) -> dict:
    """
    """
    for json_feature in features:
        if "name" in json_feature and json_feature["name"] == feature_text:
            return json_feature
    return {}


def get_records_for_patient(records: list, patient_id: str) -> list[dict]:
    """
    :param records: list of LabRecord resources
    """
    matching_records = []
    for json_record in records:
        if json_record[Record.SUBJECT_] == patient_id:
            matching_records.append(json_record)
    # also sort them by PhenFeature reference id
    log.info(matching_records)
    return sorted(matching_records, key=lambda d: d[Record.INSTANTIATES_])


def get_field_value_for_patient(records: list, features: list, patient_id: str, column_name: str) -> Any:
    """
    :param records: list of XRecord resources
    :param features: list of XFeature resource
    :param patient_id: the patient (ID) for which we want to get a specific value
    :param column_name: the column for which we want to get the value
    """

    # log.info(f"looking for the value of column {column_name} for patient {patient_id}")

    feature = None
    for one_feature in features:
        if one_feature["name"] == column_name:
            feature = one_feature
            break
    if feature is not None:
        for record in records:
            # log.info(f"checking {json_lab_record['has_subject']} vs. {patient_id} and {json_lab_record['instantiates']} vs. {feature['identifier']}")
            if record[Record.SUBJECT_] == patient_id and record[Record.INSTANTIATES_] == feature[Resource.IDENTIFIER_]:
                log.info(f"for patient {patient_id} and column {column_name}, record is {record}")
                return record[Record.VALUE_]
    return None
