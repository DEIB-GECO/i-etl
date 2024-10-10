import os
from datetime import timedelta, datetime
from typing import Any

from utils.assertion_utils import THE_DATETIME_FORMAT
from utils.setup_logger import log


# for setting up the tests with specific (env) parameters
def set_env_variables_from_dict(env_vars: dict):
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


def get_lab_feature_by_text(lab_features: list, lab_feature_text: str) -> dict:
    """
    :param lab_features: list of LabFeature instances
    """
    json_lab_features = [lab_feature.to_json() for lab_feature in lab_features]
    for json_lab_feature in json_lab_features:
        if "name" in json_lab_feature and json_lab_feature["name"] == lab_feature_text:
            return json_lab_feature
    return {}


def get_lab_records_for_patient(lab_records: list, patient_id: str) -> list[dict]:
    """
    :param lab_records: list of LabRecord resources
    """
    matching_lab_records = []
    json_lab_records_list = [lab_record.to_json() for lab_record in lab_records]
    for json_lab_record in json_lab_records_list:
        if json_lab_record["subject"] == patient_id:
            matching_lab_records.append(json_lab_record)
    # also sort them by LabFeature reference id
    return sorted(matching_lab_records, key=lambda d: d["instantiate"])


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
        if lab_feat.to_json()["name"] == column_name:
            lab_feature = lab_feat.to_json()
            break
    if lab_feature is not None:
        for lab_record in lab_records:
            json_lab_record = lab_record.to_json()
            log.info(f"checking {json_lab_record['subject']} vs. {patient_id} and {json_lab_record['instantiate']} vs. {lab_feature['identifier']}")
            if json_lab_record["subject"] == patient_id:
                if json_lab_record["instantiate"] == lab_feature["identifier"]:
                    return json_lab_record["value"]
    return None
