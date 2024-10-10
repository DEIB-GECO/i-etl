import getpass
import logging
import os.path
import platform
import subprocess
from datetime import datetime

import pymongo
from dotenv import load_dotenv

from enums.FileTypes import FileTypes
from enums.HospitalNames import HospitalNames
from enums.ParameterKeys import ParameterKeys
from enums.UpsertPolicy import UpsertPolicy
from utils import setup_logger
from constants.structure import WORKING_DIR, DB_CONNECTION, DOCKER_FOLDER_METADATA, DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS, \
    DOCKER_FOLDER_TEST
from utils.file_utils import split_list_of_files
from utils.setup_logger import log


class Execution:
    def __init__(self):
        self.execution_date = datetime.now().isoformat()

        # set up the working-dir structure based on the DB name
        self.db_name = None
        self.working_dir = os.path.join(os.getcwd(), WORKING_DIR)  # default in the code
        self.working_dir_current = None  # computed in create_current_working_dir()

        # parameters related to the project structure and the input/output files
        self.metadata_filepath = None  # user input
        self.diagnosis_regexes_filepath = None  # user input
        self.laboratory_filepaths = None  # user input
        self.sample_filepaths = None  # user input
        self.diagnosis_filepaths = None  # user input
        self.medicine_filepaths = None  # user input
        self.imaging_filepaths = None  # user input
        self.genomic_filepaths = None  # user input
        self.current_filepath = None  # set in the loop on files in ETL
        self.current_file_type = None  # set in the loop on files in ETL
        self.anonymized_patient_ids_filepath = None  # user input
        self.use_en_locale = False  # user input

        # parameters related to the database
        self.db_connection = None  # user input
        self.db_drop = True  # user input
        self.db_upsert_policy = UpsertPolicy.DO_NOTHING  # user input
        self.columns_to_remove = []  # user input

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
        self.columns_to_remove = []

    def set_up(self, setup_data_files: bool) -> None:
        log.info("in set_up")

        # A. load the env. variables defined in .env.
        # note: specifying the .env in the compose.yml only gives access to those env. var. to Docker (not to Python)
        load_dotenv(override=True)

        # B. set up env. variables into class
        self.db_connection = DB_CONNECTION  # this is not a user parameter anymore because this is part of the Docker functioning, not something user should be able to change
        self.db_name = self.check_parameter(key=ParameterKeys.DB_NAME, accepted_values=None, default_value=self.db_name)
        log.debug(f"creating new DB with name {self.db_name}")
        self.hospital_name = self.check_parameter(key=ParameterKeys.HOSPITAL_NAME, accepted_values=HospitalNames.values(), default_value=self.hospital_name)
        self.use_en_locale = self.check_parameter(key=ParameterKeys.USE_EN_LOCALE, accepted_values=["True", "False", True, False], default_value=self.use_en_locale)
        self.db_upsert_policy = self.check_parameter(key=ParameterKeys.DB_UPSERT_POLICY, accepted_values=UpsertPolicy.values(), default_value=self.db_upsert_policy)
        self.db_drop = self.check_parameter(key=ParameterKeys.DB_DROP, accepted_values=["True", "False", True, False], default_value=self.db_drop)
        self.is_extract = self.check_parameter(key=ParameterKeys.IS_EXTRACT, accepted_values=["True", "False", True, False], default_value=self.is_extract)
        self.is_transform = self.check_parameter(key=ParameterKeys.IS_TRANSFORM, accepted_values=["True", "False", True, False], default_value=self.is_transform)
        self.is_load = self.check_parameter(key=ParameterKeys.IS_LOAD, accepted_values=["True", "False", True, False], default_value=self.is_load)
        self.columns_to_remove = self.check_parameter(key=ParameterKeys.COLUMNS_TO_REMOVE_KEY, accepted_values=None, default_value=self.columns_to_remove)

        # C. create working files for the ETL
        self.create_current_working_dir()
        self.setup_logging_files()

        # D. set up the anonymized patient id data file
        # this should NOT be merged with the setup of data files as this as to be set even though no data is provided
        # (this happens in tests: data is given by hand, i.e., without set_up, but the anonymized patient IDs file still has to exist
        self.anonymized_patient_ids_filepath = self.check_parameter(key=ParameterKeys.ANONYMIZED_PATIENT_IDS, accepted_values=None, default_value=self.anonymized_patient_ids_filepath)
        log.debug(self.anonymized_patient_ids_filepath)
        if self.anonymized_patient_ids_filepath is not None:
            # it may be None when we are computing the catalogue data (because we don't need it)
            self.setup_mapping_to_anonymized_patient_ids()

        # E. set up the data and metadata files
        if setup_data_files:
            log.debug("I will also set up data files")
            self.metadata_filepath = self.check_parameter(key=ParameterKeys.METADATA_PATH, accepted_values=None, default_value=self.metadata_filepath)
            log.debug(self.metadata_filepath)
            self.diagnosis_regexes_filepath = self.check_parameter(key=ParameterKeys.DIAGNOSIS_REGEXES, accepted_values=None, default_value=self.diagnosis_regexes_filepath)
            log.debug(self.diagnosis_regexes_filepath)
            self.laboratory_filepaths = self.check_parameter(key=ParameterKeys.LABORATORY_PATHS, accepted_values=None, default_value=self.laboratory_filepaths)
            log.debug(self.laboratory_filepaths)
            self.sample_filepaths = self.check_parameter(key=ParameterKeys.SAMPLE_PATHS, accepted_values=None, default_value=self.sample_filepaths)
            log.debug(self.sample_filepaths)
            self.diagnosis_filepaths = self.check_parameter(key=ParameterKeys.DIAGNOSIS_PATHS, accepted_values=None, default_value=self.diagnosis_filepaths)
            log.debug(self.diagnosis_filepaths)
            self.medicine_filepaths = self.check_parameter(key=ParameterKeys.MEDICINE_PATHS, accepted_values=None, default_value=self.medicine_filepaths)
            log.debug(self.medicine_filepaths)
            self.imaging_filepaths = self.check_parameter(key=ParameterKeys.IMAGING_PATHS, accepted_values=None, default_value=self.imaging_filepaths)
            log.debug(self.imaging_filepaths)
            self.genomic_filepaths = self.check_parameter(key=ParameterKeys.GENOMIC_PATHS, accepted_values=None, default_value=self.genomic_filepaths)
            log.debug(self.genomic_filepaths)

            self.setup_data_files()

    def check_parameter(self, key: str, accepted_values: list|None, default_value) -> str | bool | None:
        try:
            the_parameter = os.getenv(key)
            if the_parameter is None:
                log.error(f"The parameter {key} does not exist as an environment variable. Using default value: {default_value}.")
                return default_value
            elif the_parameter == "":
                log.error(f"The parameter {key} value is empty. Using default value: {default_value}.")
                return default_value
            elif accepted_values is not None:
                if True in accepted_values and False in accepted_values:
                    if the_parameter.lower() == "true":
                        return True
                    elif the_parameter.lower() == "false":
                        return False
                    else:
                        log.error(f"The value '{the_parameter.lower()}' for parameter {key} is not accepted. Using default value: {default_value}.")
                        return default_value
                else:
                    if the_parameter not in accepted_values:
                        log.error(f"The value '{the_parameter.lower()}' for parameter {key} is not accepted. Using default value: {default_value}.")
                        return default_value
                    else:
                        return the_parameter
            else:
                return the_parameter
        except:
            log.error(f"The parameter {key} does not exist as an environment variable. Using default value: {default_value}.")
            return default_value

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
        formatter = logging.Formatter('%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
        filehandler.setFormatter(formatter)
        setup_logger.log.addHandler(filehandler)  # add the filehandler located in the working dir

    def setup_data_files(self):
        log.debug("In setup_data_files")
        # 1. compute the (Docker-rooted) absolute path to the metadata file
        if os.sep in self.metadata_filepath:
            raise ValueError(f"The provided metadata file {self.metadata_filepath} should be only the name of the metadata file, but it looks like a path.")
        if os.getenv("CONTEXT_MODE") == "TEST":
            self.metadata_filepath = os.path.join(DOCKER_FOLDER_TEST, self.metadata_filepath)
        else:
            self.metadata_filepath = os.path.join(DOCKER_FOLDER_METADATA, self.metadata_filepath)

        # 2. compute the (Docker-rooted) absolute paths to the data files
        # A. if there is a single file, this will put that single file in a list (of one element)
        #    otherwise, when the user provides several files, it will split them in an array
        # B. then, we append to each the Docker rooted data folder (because Docker can't access data in the server itself, it has to be in the shared volumes)
        # a. we process laboratory data filepaths...
        if self.laboratory_filepaths is not None:
            self.laboratory_filepaths = split_list_of_files(self.laboratory_filepaths, prefix_path=FileTypes.get_prefix_for_path(filetype=FileTypes.LABORATORY))  # file 1,file 2, ...,file N
            log.debug(f"{self.laboratory_filepaths}")

        if self.sample_filepaths is not None:
            self.sample_filepaths = split_list_of_files(self.sample_filepaths, prefix_path=FileTypes.get_prefix_for_path(filetype=FileTypes.SAMPLE))  # file 1,file 2, ...,file N
            log.debug(f"{self.sample_filepaths}")

        # b. ...diagnosis filepaths...
        if self.diagnosis_filepaths is not None:
            self.diagnosis_filepaths = split_list_of_files(self.diagnosis_filepaths, prefix_path=FileTypes.get_prefix_for_path(filetype=FileTypes.DIAGNOSIS))  # file 1,file 2, ...,file N
            log.debug(f"{self.diagnosis_filepaths}")

        # c. ...medicine filepaths ...
        if self.medicine_filepaths is not None:
            self.medicine_filepaths = split_list_of_files(self.medicine_filepaths, prefix_path=FileTypes.get_prefix_for_path(filetype=FileTypes.MEDICINE))  # file 1,file 2, ...,file N
            log.debug(f"{self.medicine_filepaths}")

        # d. ...imaging filepaths ...
        if self.imaging_filepaths is not None:
            self.imaging_filepaths = split_list_of_files(self.imaging_filepaths, prefix_path=FileTypes.get_prefix_for_path(filetype=FileTypes.IMAGING))  # file 1,file 2, ...,file N
            log.debug(f"{self.imaging_filepaths}")

        # e. ...genomic filepaths
        if self.genomic_filepaths is not None:
            self.genomic_filepaths = split_list_of_files(self.genomic_filepaths, prefix_path=FileTypes.get_prefix_for_path(filetype=FileTypes.GENOMIC))  # file 1,file 2, ...,file N
            log.debug(f"{self.genomic_filepaths}")

        # f. ... diagnosis regexes
        if self.diagnosis_regexes_filepath is not None:
            self.diagnosis_regexes_filepath = os.path.join(FileTypes.get_prefix_for_path(filetype=FileTypes.DIAGNOSIS_REGEX), self.diagnosis_regexes_filepath)
            log.debug(f"{self.diagnosis_regexes_filepath}")

    def setup_mapping_to_anonymized_patient_ids(self):
        if os.sep in self.anonymized_patient_ids_filepath:
            raise ValueError(f"The anonymized patient ids ({self.anonymized_patient_ids_filepath}) file should be a filename, but it looks like a filepath.")
        else:
            self.anonymized_patient_ids_filepath = os.path.join(DOCKER_FOLDER_TEST if os.getenv("CONTEXT_MODE") == "TEST" else DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS, self.anonymized_patient_ids_filepath)
            log.debug(self.anonymized_patient_ids_filepath)
            if not os.path.exists(self.anonymized_patient_ids_filepath) or os.stat(self.anonymized_patient_ids_filepath).st_size == 0:
                log.info("write {} in patient ids mapping")
                # the file is empty, we simply add the empty mapping
                # otherwise the file cannot be read as a JSON file
                with open(self.anonymized_patient_ids_filepath, "w") as file:
                    file.write("{}")
            else:
                log.info("patient ids mapping already contains data")
                # there are some mappings there, nothing more to do
                pass

    def to_json(self):
        return {
            # "identifier": self.identifier.to_json(),  # TODO Nelly: check how to number Execution instances
            "user_parameters": {
                "working_dir": self.working_dir,
                "working_dir_current": self.working_dir_current,
                "current_filepath": self.current_filepath,
                ParameterKeys.METADATA_PATH: self.metadata_filepath,
                ParameterKeys.LABORATORY_PATHS: self.laboratory_filepaths,
                ParameterKeys.SAMPLE_PATHS: self.sample_filepaths,
                ParameterKeys.DIAGNOSIS_PATHS: self.diagnosis_filepaths,
                ParameterKeys.MEDICINE_PATHS: self.medicine_filepaths,
                ParameterKeys.IMAGING_PATHS: self.imaging_filepaths,
                ParameterKeys.GENOMIC_PATHS: self.genomic_filepaths,
                ParameterKeys.ANONYMIZED_PATIENT_IDS: self.anonymized_patient_ids_filepath,
                ParameterKeys.DB_NAME: self.db_name,
                ParameterKeys.DB_DROP: self.db_drop,
                ParameterKeys.HOSPITAL_NAME: self.hospital_name,
            },
            "execution_context": {
                "python_version": self.python_version,
                "pymongo_version": self.pymongo_version,
                "execution_date": self.execution_date,
                "platform": self.platform,
                "platform_version": self.platform_version,
                "user": self.user
            }
        }
