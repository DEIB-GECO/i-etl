import os

import pandas as pd
import ujson

from constants.structure import GROUND_DATA_FOLDER_FOR_GENERATION, GROUND_METADATA_FOLDER_FOR_GENERATION
from enums.TableNames import TableNames
from utils.setup_logger import log


def write_in_file(resource_list: list, current_working_dir: str, profile: str, is_feature: bool, dataset_number: int, file_counter: int, to_json: bool) -> None:
    if profile in [TableNames.PATIENT, TableNames.HOSPITAL, TableNames.TEST]:
        table_name = profile
    elif is_feature:
        table_name = TableNames.FEATURE
    else:
        table_name = TableNames.RECORD
    filename = get_json_resource_file(current_working_dir=current_working_dir, profile=profile, table_name=table_name, dataset_number=dataset_number)
    if len(resource_list) > 0:
        with open(filename, "w") as data_file:
            try:
                # log.debug(f"Dumping {len(resource_list)} instances in {filename}")
                if to_json:
                    the_json_resources = [resource.to_json() for resource in resource_list]
                    processed_json = ujson.dumps(the_json_resources)
                else:
                    processed_json = ujson.dumps(resource_list)
                processed_json = processed_json[1:-1]
                processed_json = processed_json.replace("},{", "}\n{")
                log.info(processed_json)
                data_file.write(processed_json)
            except Exception:
                raise ValueError(f"Could not dump the {len(resource_list)} JSON resources in the file located at {filename}.")
    else:
        log.info(f"No data when writing file {filename}.")


def get_json_resource_file(current_working_dir: str, dataset_number: int, profile: str, table_name: str) -> str:
    return os.path.join(current_working_dir, f"{str(dataset_number)}{profile}{table_name}.json")


def read_tabular_file_as_string(filepath: str) -> pd.DataFrame:
    if filepath.endswith(".csv"):
        # leave empty cells as '' cells (they will be skipped during the Transform iteration on data values)
        # keep cells with explicit NaN values as they are (they will be converted into NaN during the Transform iteration on data values)
        # following issue #269
        return pd.read_csv(filepath, index_col=False, dtype=str, na_values=[], keep_default_na=False)
    elif filepath.endswith(".xls") or filepath.endswith(".xlsx"):
        # for Excel files, there may be several sheets, so we load all data in a single dataframe
        all_sheets = pd.read_excel(filepath, sheet_name=None, index_col=False, dtype=str, na_values=[], keep_default_na=False)
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
