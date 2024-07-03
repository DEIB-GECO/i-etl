import getpass
import logging
import os.path
import platform
import shutil
from datetime import datetime

import pymongo
from argparse import Namespace

from utils import setup_logger
from utils.constants import WORKING_DIR, DEFAULT_DB_NAME, DEFAULT_DB_CONNECTION
from utils.setup_logger import log
from utils.utils import is_not_empty


class Execution:
    # parameters related to the project structure and the input/output files
    # parameters related to the database
    # parameters related to the UC hospital
    HOSPITAL_NAME_KEY = "hospital_name"
    # parameters related to the execution context (python, pymongo, etc)
    PYTHON_VERSION_KEY = "python_version"
    PYMONGO_VERSION_KEY = "pymongo_version"
    EXECUTION_DATE_KEY = "execution_date"
    PLATFORM_KEY = "platform"
    PLATFORM_VERSION_KEY = "platform_version"
    USER_KEY = "user"
    USE_EN_LOCALE_KEY = "locale"
    # parameters related to the ETL pipeline
    EXTRACT_KEY = "extract"
    TRANSFORM_KEY = "transform"
    LOAD_KEY = "load"
    ANALYSIS_KEY = "analysis"

    def __init__(self, db_name: str):
        self.execution_date = datetime.now().isoformat()

        # set up the working-dir structure based on the DB name
        self.db_name = db_name
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
        # parameters related to the UC hospital
        self.hospital_name = None  # this will be given as input by users
        # parameters related to the execution context (python, pymongo, etc)
        self.python_version = platform.python_version()
        self.pymongo_version = pymongo.version
        self.platform = platform.platform()
        self.platform_version = platform.version()
        self.user = getpass.getuser()
        # parameters related to the ETL pipeline
        self.extract = True
        self.transform = True
        self.load = True
        self.analyze = False

    def set_up_with_user_params(self, args: Namespace) -> None:
        self.set_up(args.__dict__, True)

    def set_up(self, args_as_dict: dict, setup_data_files: bool) -> None:
        # A. set up user parameters
        if "hospital_name" in args_as_dict:
            self.hospital_name = args_as_dict["hospital_name"]
        if "use_en_locale" in args_as_dict:
            self.use_en_locale = args_as_dict["use_en_locale"]
        if "db_connection" in args_as_dict:
            self.db_connection = args_as_dict["db_connection"]
        # the boolean parameters need to be compared to True (and not False),
        # even if the default value is false, because:
        # "True" == "True" -> true
        # "False" == "False" -> true but here we want False
        if "db_drop" in args_as_dict:
            self.db_drop = args_as_dict["db_drop"] == "True"
        if "db_no_index" in args_as_dict:
            self.db_no_index = args_as_dict["db_no_index"] == "True"
        if "extract" in args_as_dict:
            self.extract = args_as_dict["extract"] == "True"
        if "analysis" in args_as_dict:
            self.analyze = args_as_dict["analyze"] == "True"
        if "transform" in args_as_dict:
            self.transform = args_as_dict["transform"] == "True"
        if "load" in args_as_dict:
            self.load = args_as_dict["load"] == "True"

        # B. set up the data and metadata files
        if setup_data_files:
            if "clinical_metadata_filepath" in args_as_dict:
                self.clinical_metadata_filepath = args_as_dict["clinical_metadata_filepath"]
            if "clinical_filepaths" in args_as_dict:
                self.clinical_filepaths = args_as_dict["clinical_filepaths"]
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
        print(self.working_dir_current)
        log_file = os.path.join(self.working_dir_current, f"log-{self.execution_date}.log")
        filehandler = logging.FileHandler(log_file, 'a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        filehandler.setFormatter(formatter)
        setup_logger.log.addHandler(filehandler)  # add the filehandler located in the working dir

    def setup_data_files(self):
        # get metadata and data filepaths
        if self.clinical_metadata_filepath is None or not os.path.isfile(self.clinical_metadata_filepath):
            raise FileNotFoundError("The specified metadata file '%s' does not exist.", self.clinical_metadata_filepath)
        else:
            new_metadata_filename = "metadata-" + self.hospital_name + ".csv"
            new_clinical_metadata_filepath = os.path.join(self.working_dir_current, new_metadata_filename)
            shutil.copyfile(self.clinical_metadata_filepath, new_clinical_metadata_filepath)
            self.clinical_metadata_filepath = new_clinical_metadata_filepath

        # if there is a single file, this will put that file in a list
        # otherwise, when the user provides several files, it will split them in an array
        log.debug(f"{self.clinical_filepaths}")
        if self.clinical_filepaths is None:
            raise FileNotFoundError("No clinical data file has been provided.")
        else:
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
                and self.extract is not None
                and self.transform is not None
                and self.load is not None
                and self.analyze is not None)

    def set_clinical_metadata_filepath(self, clinical_metadata_filepath: str) -> None:
        if is_not_empty(clinical_metadata_filepath):
            self.clinical_metadata_filepath = clinical_metadata_filepath
        else:
            raise ValueError("The metadata filepath cannot be set in the config because it is None or empty.")

    def set_clinical_filepaths(self, clinical_filepaths: str) -> None:
        # clinical_filepaths is a set of data filepaths, concatenated with commas (,)
        # this is what we get from the user input parameters
        if is_not_empty(clinical_filepaths):
            self.clinical_filepaths = clinical_filepaths.split(",")
        else:
            raise ValueError("The data filepaths cannot be set in the config because it is None or empty.")

    def set_db_connection(self, db_connection: str) -> None:
        if is_not_empty(db_connection):
            self.db_connection = db_connection
        else:
            raise ValueError("The db connection string cannot be set in the config because it is None or empty.")

    def set_db_name(self, db_name: str) -> None:
        if is_not_empty(db_name):
            self.db_name = db_name
        else:
            raise ValueError("The db name string cannot be set in the config because it is None or empty.")

    def set_db_drop(self, drop: bool) -> None:
        if is_not_empty(drop):
            self.db_drop = drop
        else:
            raise ValueError("The drop parameter cannot be set in the config because it is None or empty.")

    def set_no_index(self, no_index: bool) -> None:
        if is_not_empty(no_index):
            self.db_no_index = no_index
        else:
            raise ValueError("The no_index parameter cannot be set in the config because it is None or empty.")

    def set_hospital_name(self, hospital_name: str) -> None:
        if is_not_empty(hospital_name):
            self.hospital_name = hospital_name
        else:
            raise ValueError("The hospital name parameter cannot be set in the config because it is None or empty.")

    def set_extract(self, extract: bool) -> None:
        self.extract = extract

    def set_analyze(self, analyze: bool) -> None:
        self.analyze = analyze

    def set_transform(self, transform: bool) -> None:
        self.transform = transform

    def set_load(self, load: bool) -> None:
        self.load = load

    # get execution variables
    def get_working_dir(self) -> str:
        return self.working_dir

    def get_working_dir_current(self) -> str:
        return self.working_dir_current

    def get_clinical_metadata_filepath(self) -> str:
        return self.clinical_metadata_filepath

    def get_current_filepath(self) -> str:
        return self.current_filepath

    def set_current_filepath(self, current_filepath: str) -> None:
        self.current_filepath = current_filepath

    def get_clinical_filepaths(self) -> list[str]:
        return self.clinical_filepaths

    def get_db_connection(self) -> str:
        return self.db_connection

    def get_db_name(self) -> str:
        return self.db_name

    def get_db_drop(self) -> bool:
        return self.db_drop

    def get_no_index(self) -> bool:
        return self.db_no_index

    def get_hospital_name(self) -> str:
        return self.hospital_name

    def get_pymongo_version(self) -> str:
        return self.pymongo_version

    def get_execution_date(self) -> str:
        return self.execution_date

    def get_platform(self) -> str:
        return self.platform

    def get_platform_version(self) -> str:
        return self.platform_version

    def get_user(self) -> str:
        return self.user

    def get_use_en_locale(self) -> bool:
        return self.use_en_locale

    def get_extract(self) -> bool:
        return self.extract

    def get_transform(self) -> bool:
        return self.transform

    def get_load(self) -> bool:
        return self.load

    def get_analyze(self) -> bool:
        return self.analyze

    def to_json(self):
        return {
            # "identifier": self.identifier.to_json(),  # TODO Nelly: check how to number Execution instances
            "user_parameters": {
                "working_dir": self.working_dir,
                "working_dir_current": self.working_dir_current,
                "clinical_metadata_filepath": self.clinical_metadata_filepath,
                "clinical_filepaths": self.clinical_filepaths,
                "current_filepath": self.current_filepath,
                "db_connection": self.db_connection,
                "db_name": self.db_name,
                "db_drop": self.db_drop,
                "hospital_name": self.hospital_name,
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
