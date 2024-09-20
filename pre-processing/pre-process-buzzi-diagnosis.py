import sys

from utils.assertion_utils import is_not_nan
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log

if __name__ == '__main__':
    filepath = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/datasets/data/BUZZI/diagnoses-cleaned.xlsx"  # sys.argv[1]
    new_filepath = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/datasets/data/BUZZI/diagnoses-pre-processed.csv"  # sys.argv[2]
    df = read_tabular_file_as_string(filepath=filepath)

    log.info(df)

    diagnosis = [row["affetto"] if is_not_nan(row["affetto"]) else row["carrier"] if is_not_nan(row["carrier"]) else None for index, row in df.iterrows()]
    log.info(diagnosis)
    affected_booleans = [True if is_not_nan(row["affetto"]) else False for index, row in df.iterrows()]
    log.info(affected_booleans)

    df["acronym"] = diagnosis
    df["affected"] = affected_booleans

    for index, row in df.iterrows():
        if is_not_nan(row["affetto"]) and is_not_nan(row["carrier"]):
            log.info(row)
        if not is_not_nan(row["affetto"]) and not is_not_nan(row["carrier"]):
            log.info(row)

    log.info(df.to_string())

    df.to_csv(new_filepath, index=False)
