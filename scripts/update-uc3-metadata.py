from enums.MetadataColumns import MetadataColumns
from utils.file_utils import read_tabular_file_as_string

if __name__ == '__main__':
    current_md = read_tabular_file_as_string(filepath="~/Downloads/UC3-variables-02-04-2025.xlsx")
    latest_md = read_tabular_file_as_string(filepath="~/Downloads/UC3 DAT_BBDD_DESCRIPTION_02042025.xlsx")

    current_variables = list(current_md[MetadataColumns.COLUMN_NAME])
    latest_variables = list(latest_md[MetadataColumns.COLUMN_NAME])

    current_variables = [variable.lower() for variable in current_variables]
    latest_variables = [variable.lower() for variable in latest_variables]

    current_and_latest = list(set(current_variables) & set(latest_variables))
    current_not_in_latest = [current_var for current_var in current_variables if current_var not in latest_variables]
    latest_not_in_current = [latest_var for latest_var in latest_variables if latest_var not in current_variables]

    print(f"There are {len(current_variables)} current variables")  #: {current_variables}")
    print(f"There are {len(latest_variables)} latest variables")  #: {latest_variables}")
    print(f"{len(current_and_latest)} variables are both in current and latest: {current_and_latest}")
    print(f"{len(current_not_in_latest)} variables are in current but not in latest: {current_not_in_latest}")
    print(f"{len(latest_not_in_current)} variables are in latest but not in current: {latest_not_in_current}")

