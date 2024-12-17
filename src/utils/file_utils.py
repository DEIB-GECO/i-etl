import os

import pandas as pd
import ujson

from constants.structure import GROUND_DATA_FOLDER_FOR_GENERATION, GROUND_METADATA_FOLDER_FOR_GENERATION
from enums.TableNames import TableNames
from utils.setup_logger import log


def write_in_file(resource_list: list, current_working_dir: str, profile: str, is_feature: bool, dataset_number: int, file_counter: int) -> None:
    if profile in [TableNames.PATIENT, TableNames.HOSPITAL, TableNames.TEST]:
        table_name = profile
    elif is_feature:
        table_name = TableNames.FEATURE
    else:
        table_name = TableNames.RECORD
    filename = get_json_resource_file(current_working_dir=current_working_dir, profile=profile, table_name=table_name, dataset_number=dataset_number, file_counter=file_counter)
    if len(resource_list) > 0:
        with open(filename, "w") as data_file:
            try:
                log.debug(f"Dumping {len(resource_list)} instances in {filename}")
                ujson.dump([resource.to_json() for resource in resource_list], data_file)
            except Exception:
                raise ValueError(f"Could not dump the {len(resource_list)} JSON resources in the file located at {filename}.")
    else:
        log.info(f"No data when writing file {filename}.")


def get_json_resource_file(current_working_dir: str, dataset_number: int, profile: str, table_name: str, file_counter: int) -> str:
    return os.path.join(current_working_dir, f"{str(dataset_number)}{profile}{table_name}{str(file_counter)}.json")


def read_tabular_file_as_string(filepath: str) -> pd.DataFrame:
    # na_values is the list containing values to be recognized as NA.
    # By default, there are " ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null".
    # I extend it with "no information", "No information"
    na_values = ["no information", "No information", "No Information", "-", "0", "0.0", "-0", "-0.0"]
    if filepath.endswith(".csv"):
        return pd.read_csv(filepath, index_col=False, dtype=str, na_values=na_values, keep_default_na=True)
    elif filepath.endswith(".xls") or filepath.endswith(".xlsx"):
        # for Excel files, there may be several sheets, so we load all data in a single dataframe
        all_sheets = pd.read_excel(filepath, sheet_name=None, index_col=False, dtype=str, na_values=na_values, keep_default_na=True)
        all_sub_df = []
        for key, value in all_sheets.items():
            if key != "Legend":  # skip the sheet describing the columns
                all_sub_df.append(value)
        return pd.concat(all_sub_df, ignore_index=True, axis="rows")  # append lines, not vertically as new columns
    else:
        raise ValueError(f"The extension of the tabular file {filepath} is not recognised. Accepted extensions are .csv, .xls, and .xlsx.")


def get_ground_data(filename: str) -> str:
    return os.path.join(GROUND_DATA_FOLDER_FOR_GENERATION, filename)


def get_ground_metadata(filename: str) -> str:
    return os.path.join(GROUND_METADATA_FOLDER_FOR_GENERATION, filename)
