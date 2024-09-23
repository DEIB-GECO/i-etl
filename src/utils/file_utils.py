import json
import os

import pandas as pd
from pandas import DataFrame

from utils.setup_logger import log


def write_in_file(resource_list: list, current_working_dir: str, table_name: str, count: int) -> None:
    filename = get_json_resource_file(current_working_dir=current_working_dir, table_name=table_name, count=count)
    if len(resource_list) > 0:
        with open(filename, "w") as data_file:
            try:
                log.debug(f"Dumping {len(resource_list)} instances in {filename}")
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
        # for Excel files, there may be several sheets, so we load all data in a single dataframe
        all_sheets = pd.read_excel(filepath, sheet_name=None, index_col=False, dtype=str, keep_default_na=True)
        df = DataFrame()
        for key, value in all_sheets.items():
            if key != "Legend":  # skip the sheet describing the columns
                df = pd.concat([df, value], ignore_index=True)
        return df
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
