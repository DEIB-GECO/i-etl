import os
import re

import pandas as pd
import jsonlines

from enums.TableNames import TableNames
from utils.setup_logger import log


def write_in_file(resource_list: list, current_working_dir: str, table_name: str, is_feature: bool, dataset_id: int, to_json: bool) -> None:
    if table_name not in [TableNames.PATIENT, TableNames.HOSPITAL, TableNames.TEST]:
        if is_feature:
            table_name = TableNames.FEATURE
        else:
            table_name = TableNames.RECORD
    filename = get_json_resource_file(current_working_dir=current_working_dir, table_name=table_name, dataset_id=dataset_id)
    if len(resource_list) > 0:
        with jsonlines.open(filename, "a") as data_file:
            try:
                # log.debug(f"Dumping {len(resource_list)} instances in {filename}")
                if to_json:
                    data_file.write_all([resource.to_json() for resource in resource_list])
                    # the_json_resources = [resource.to_json() for resource in resource_list]
                    # the_json_string = ujson.dumps(the_json_resources)
                else:
                    data_file.write_all(resource_list)
                    # the_json_resources = resource_list
                # here we write a JSONL, meaning that each record is on a line
                # there is not encompassing brackets (array), not commas between records
                # json_string = from_json_str_to_json_line(the_json_string)
                # log.info(json_string)
                # data_file.write(json_string)
            except Exception:
                raise ValueError(f"Could not dump the {len(resource_list)} JSON resources in the file located at {filename}.")
    else:
        log.info(f"No data when writing file {filename}.")


def get_json_resource_file(current_working_dir: str, dataset_id: int, table_name: str) -> str:
    return os.path.join(current_working_dir, f"{str(dataset_id)}{table_name}.jsonl")


def clear_file(current_working_dir: str, dataset_id: int, table_name: str) -> None:
    existing_file = get_json_resource_file(current_working_dir=current_working_dir,
                                           dataset_id=dataset_id,
                                           table_name=table_name)
    with open(existing_file, "w") as f:
        f.write("")


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


# transform a json-line file (one record per line, no comma, no encompassing brackets) to a valid stringified json (with brackets and commas)
def from_json_line_to_json_str(json_file) -> str:
    read_json = json_file.read()  # this is a JSON-by-line file, we need to append the encompassing brackets and a comma between each record
    read_json = "[" + read_json + "]"
    return re.sub("}\n\\{", "},{", read_json)


# transform a stringified json (e.g., outputed by json.dumps) into a str containing one record per line, no comma, and no encompassing brackets
def from_json_str_to_json_line(json_str) -> str:
    # be careful here: the easy solution is to replace },{ by }\n{
    # however, if there are nested elements, such pattern can appear in an element while not being the end of the record
    # therefore the regex needs to be tightened
    return json_str[1:-1].replace("},{", "}\n{")  # remove the brackets from the list and move each record on a line
