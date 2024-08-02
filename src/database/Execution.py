import getpass
import logging
import os.path
import platform
import shutil
from argparse import Namespace
from datetime import datetime

import pymongo

from utils import setup_logger
from enums.UpsertPolicy import UpsertPolicy
from utils.constants import WORKING_DIR, DEFAULT_DB_CONNECTION
from utils.setup_logger import log


class Execution:
    HOSPITAL_NAME_KEY = "hospital_name"
    USE_EN_LOCALE_KEY = "use_en_locale"
    DB_CONNECTION_KEY = "db_connection"
    DB_NAME_KEY = "db_name"
    DB_DROP_KEY = "db_drop"
    DB_UPSERT_POLICY_KEY = "db_upsert_policy"
    IS_EXTRACT_KEY = "extract"
    IS_ANALYZE_KEY = "analyze"
    IS_TRANSFORM_KEY = "transform"
    IS_LOAD_KEY = "load"
    CLINICAL_METADATA_PATH_KEY = "clinical_metadata_path"
    CLINICAL_DATA_PATHS_KEY = "clinical_data_paths"

    def __init__(self, db_name: str):
        self.execution_date = datetime.now().isoformat()

        # set up the working-dir structure based on the DB name
        self.db_name = db_name
        log.debug(f"creating new DB with name {self.db_name}")
        self.working_dir = os.path.join(os.getcwd(), WORKING_DIR)  # default in the code
        self.working_dir_current = None  # computed in create_current_working_dir()
        self.create_current_working_dir()
        self.setup_logging_files()

        # parameters related to the project structure and the input/output files
        self.clinical_metadata_filepath = None  # user input
        self.clinical_filepaths = None  # user input
        self.current_filepath = None  # computed in setup
        self.use_en_locale = True  # user input
        # parameters related to the database
        self.db_connection = DEFAULT_DB_CONNECTION  # user input
        self.db_drop = True  # user input
        self.db_no_index = False  # user input
        self.db_upsert_policy = UpsertPolicy.DO_NOTHING  # user input
        # parameters related to the UC hospital
        self.hospital_name = None  # this will be given as input by users
        # parameters related to the execution context (python, pymongo, etc.)
        self.python_version = platform.python_version()
        self.pymongo_version = pymongo.version
        self.platform = platform.platform()
        self.platform_version = platform.version()
        self.user = getpass.getuser()
        # parameters related to the ETL pipeline
        self.is_extract = True
        self.is_transform = True
        self.is_load = True
        self.is_analyze = False

    def set_up_with_user_params(self, args: Namespace) -> None:
        self.set_up(args.__dict__, True)

    def set_up(self, args_as_dict: dict, setup_data_files: bool) -> None:
        log.debug("in set_up")
        # A. set up user parameters
        if Execution.HOSPITAL_NAME_KEY in args_as_dict:
            self.hospital_name = args_as_dict[Execution.HOSPITAL_NAME_KEY]
        if Execution.USE_EN_LOCALE_KEY in args_as_dict:
            self.use_en_locale = args_as_dict[Execution.USE_EN_LOCALE_KEY]
        if Execution.DB_CONNECTION_KEY in args_as_dict:
            self.db_connection = args_as_dict[Execution.DB_CONNECTION_KEY]
        if Execution.DB_UPSERT_POLICY_KEY in args_as_dict:
            self.db_upsert_policy = args_as_dict[Execution.DB_UPSERT_POLICY_KEY]
        # the boolean parameters need to be compared to True (and not False),
        # even if the default value is false, because:
        # "True" == "True" -> true
        # "False" == "False" -> true but here we want False
        if Execution.DB_DROP_KEY in args_as_dict:
            self.db_drop = args_as_dict[Execution.DB_DROP_KEY] == "True" or args_as_dict[Execution.DB_DROP_KEY] is True
        if Execution.IS_EXTRACT_KEY in args_as_dict:
            self.is_extract = args_as_dict[Execution.IS_EXTRACT_KEY] == "True" or args_as_dict[Execution.IS_EXTRACT_KEY] is True
        if Execution.IS_ANALYZE_KEY in args_as_dict:
            self.is_analyze = args_as_dict[Execution.IS_ANALYZE_KEY] == "True" or args_as_dict[Execution.IS_ANALYZE_KEY] is True
        if Execution.IS_TRANSFORM_KEY in args_as_dict:
            self.is_transform = args_as_dict[Execution.IS_TRANSFORM_KEY] == "True" or args_as_dict[Execution.IS_TRANSFORM_KEY] is True
        if Execution.IS_LOAD_KEY in args_as_dict:
            self.is_load = args_as_dict[Execution.IS_LOAD_KEY] == "True" or args_as_dict[Execution.IS_LOAD_KEY] is True

        # B. set up the data and metadata files
        if setup_data_files:
            log.debug("I will also set up data files")
            if Execution.CLINICAL_METADATA_PATH_KEY in args_as_dict:
                self.clinical_metadata_filepath = args_as_dict[Execution.CLINICAL_METADATA_PATH_KEY]
            log.debug(self.clinical_metadata_filepath)
            if Execution.CLINICAL_DATA_PATHS_KEY in args_as_dict:
                self.clinical_filepaths = args_as_dict[Execution.CLINICAL_DATA_PATHS_KEY]
            log.debug(self.clinical_filepaths)
            self.setup_data_files()

    def create_current_working_dir(self):
        # 1. check whether the folder working-dir exists, if not create it
        current_path = os.getcwd()
        working_dir = os.path.join(current_path, WORKING_DIR)
        if not os.path.exists(working_dir):
            log.info(f"Creating the working dir at {working_dir}")
            os.makedirs(working_dir)
        # 2. check whether the db folder exists, if not create it
        working_dir_with_db = os.path.join(working_dir, self.db_name)
        if not os.path.exists(working_dir_with_db):
            log.info(f"Creating a sub-folder for the current database at {working_dir_with_db}")
            os.makedirs(working_dir_with_db)
        # 3. check whether the execution folder exists, if not create it
        execution_folder = os.path.join(working_dir_with_db, self.execution_date)
        if not os.path.exists(execution_folder):
            log.info(f"Creating a sub-sub-folder for the current execution at {execution_folder}")
            os.makedirs(execution_folder)
        self.working_dir_current = execution_folder

    def setup_logging_files(self):
        log_file = os.path.join(self.working_dir_current, f"log-{self.execution_date}.log")
        filehandler = logging.FileHandler(log_file, 'a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        filehandler.setFormatter(formatter)
        setup_logger.log.addHandler(filehandler)  # add the filehandler located in the working dir

    def setup_data_files(self):
        log.debug("In setup_data_files")
        # get metadata and data filepaths
        try:
            new_metadata_filename = "metadata-" + self.hospital_name + ".csv"
            new_clinical_metadata_filepath = os.path.join(self.working_dir_current, new_metadata_filename)
            shutil.copyfile(self.clinical_metadata_filepath, new_clinical_metadata_filepath)
            self.clinical_metadata_filepath = new_clinical_metadata_filepath
        except Exception:
            raise FileNotFoundError(f"The specified metadata file {self.clinical_metadata_filepath} does not exist.")

        # if there is a single file, this will put that file in a list
        # otherwise, when the user provides several files, it will split them in an array
        log.debug(f"{self.clinical_filepaths}")
        split_files = self.clinical_filepaths.split(",")
        log.debug(f"{split_files}")
        for current_file in split_files:
            if not os.path.isfile(current_file):
                raise FileNotFoundError("The specified data file " + current_file + " does not exist.")
        # we do not copy the data in our working dir because it is too large to be copied
        self.clinical_filepaths = split_files  # file 1,file 2, ...,file N
        log.debug(f"{self.clinical_filepaths}")

    def has_no_none_attributes(self) -> bool:
        return (self.working_dir is not None
                and self.working_dir_current is not None
                and self.clinical_metadata_filepath is not None
                and self.clinical_filepaths is not None
                and self.use_en_locale is not None
                and self.db_name is not None
                and self.db_connection is not None
                and self.db_drop is not None
                and self.db_no_index is not None
                and self.hospital_name is not None
                and self.is_extract is not None
                and self.is_transform is not None
                and self.is_load is not None
                and self.is_analyze is not None)

    def to_json(self):
        return {
            # "identifier": self.identifier.to_json(),  # TODO Nelly: check how to number Execution instances
            "user_parameters": {
                "working_dir": self.working_dir,
                "working_dir_current": self.working_dir_current,
                Execution.CLINICAL_METADATA_PATH_KEY: self.clinical_metadata_filepath,
                Execution.CLINICAL_DATA_PATHS_KEY: self.clinical_filepaths,
                "current_filepath": self.current_filepath,
                Execution.DB_CONNECTION_KEY: self.db_connection,
                Execution.DB_NAME_KEY: self.db_name,
                Execution.DB_DROP_KEY: self.db_drop,
                Execution.HOSPITAL_NAME_KEY: self.hospital_name,
            },
            "execution_context": {
                "python_version": self.python_version,
                "pymongo_version": self.pymongo_version,
                "execution_date": self.execution_date,
                "platform": self.platform,
                "platform_version": self.platform_version,
                "user": self.user
            },
            # "analysis": self.execution_analysis.to_json()  # TODO Nelly: uncomment this
        }
