import os
import urllib

import pandas as pd
from pandas import DataFrame

from src.utils.HospitalNames import HospitalNames
from src.utils.MetadataUrls import MetadataUrls
from src.utils.Utils import is_in_insensitive
from src.utils.constants import METADATA_VARIABLES
from src.utils.setup_logger import log


def prepare_for_etl(metadata_filename: str, hospital_name: str):
    download_data(hospital_name)
    # log.debug(hospital_name)
    # split_metadata_per_hospital(metadata_filename, hospital_name)


def download_data(hospital_name: str):
    hospital_enum_key = HospitalNames(hospital_name).name
    metadata_url = MetadataUrls[hospital_enum_key].value
    urllib.request.urlretrieve(metadata_url, os.path.join("working-dir", "metadata", "metadata-" + hospital_name + ".csv"))


def split_metadata_per_hospital(metadata_filename: str, hospital_name: str):
    metadata_df = pd.read_csv(metadata_filename)

    # 1. capitalize and replace spaces in column names
    metadata_df.rename(columns=lambda x: x.upper().replace(" ", "_"), inplace=True)

    # 2. for each hospital, get its associated metadata
    log.debug("working on hospital %s", hospital_name)
    hospital_df = DataFrame(metadata_df)
    # a. we remove columns that are talking about other hospitals, and keep metadata variables + the column for the current hospital
    columns_to_keep = []
    columns_to_keep.extend([meta_variable.upper().replace(" ", "_") for meta_variable in METADATA_VARIABLES])
    columns_to_keep.append(hospital_name)
    log.debug(hospital_df.columns)
    log.debug(columns_to_keep)
    hospital_df = hospital_df[columns_to_keep]
    # b. we filter metadata that is not part of the current hospital (to avoid having the whole metadata for each hospital)
    hospital_df = hospital_df[hospital_df[hospital_name] == 1]
    # c. we remove the column for the hospital, now that we have filtered the rows using it
    log.debug("will drop %s in %s", hospital_name, hospital_df.columns)
    hospital_df = hospital_df.drop(hospital_name, axis=1)
    log.debug(hospital_df)
    hospital_df.to_csv(os.path.join("working-dir", "metadata", "metadata-" + hospital_name + ".csv"), sep=',')
