import dataclasses
import getpass
import json
import logging
import os.path
import platform
from dataclasses import field
from datetime import datetime

import pymongo

from constants.methods import factory
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile

from constants.structure import WORKING_DIR, DB_CONNECTION, DOCKER_FOLDER_METADATA, DOCKER_FOLDER_TEST, DEFAULT_DB_NAME
from enums.HospitalNames import HospitalNames
from enums.ParameterKeys import ParameterKeys
from utils import setup_logger
from utils.cast_utils import cast_str_to_int
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class Execution:
    execution_date: datetime = datetime.now()
    working_dir: str = field(init=False, default=os.path.join(os.getcwd(), WORKING_DIR))  # default in the code
    working_dir_current: str = field(init=False, default=None)  # computed in create_current_working_dir()

    # parameters related to the project structure and the input/output files
    metadata_filepath: str = field(init=False, default=None)  # user input
    current_filepath: str = field(init=False, default=None)  # set in the loop on files in ETL
    current_dataset_gid: str = field(init=False, default=None)  # set in the loop on file sin ETL
    current_file_number: int = field(init=False, default=1)  # set in the ETL
    anonymized_patient_ids_filepath: str = field(init=False, default=None)  # user input
    use_locale: str = field(init=False, default="en_GB")  # user input
    record_carrier_patients: bool = field(init=False, default=False)  # user input

    # parameters related to the database and the ETL
    hospital_name: str = field(init=False, default="")  # this will be given as input by users
    db_connection: str = field(init=False, default="mongodb://localhost:27018/")  # user input
    db_name: str = field(init=False, default=DEFAULT_DB_NAME)
    db_drop: bool = field(init=False, default=False)  # user input
    columns_to_remove: list = field(init=False, default_factory=list)  # user input
    patient_id_column_name: str = field(init=False, default="id")
    sample_id_column_name: str = field(init=False, default="")

    # parameters related to the execution context (python, pymongo, etc.)
    python_version: str = platform.python_version()
    pymongo_version: str = pymongo.version
    platform: str = platform.platform()
    user: str = getpass.getuser()

    def internals_set_up(self) -> None:
        log.info("in set_up")

        # set class variables using env. variables
        self.db_connection = DB_CONNECTION  # this is not a user parameter anymore because this is part of the Docker functioning, not something user should be able to change
        self.db_name = self.check_parameter(key=ParameterKeys.DB_NAME, accepted_values=None, default_value=self.db_name)
        log.debug(f"creating new DB with name {self.db_name}")
        log.info(HospitalNames.values())
        self.hospital_name = self.check_parameter(key=ParameterKeys.HOSPITAL_NAME, accepted_values=HospitalNames.values(), default_value=self.hospital_name)
        self.use_locale = self.check_parameter(key=ParameterKeys.USE_LOCALE, accepted_values=None, default_value=self.use_locale)
        self.db_drop = self.check_parameter(key=ParameterKeys.DB_DROP, accepted_values=["True", "False", True, False], default_value=self.db_drop)
        self.columns_to_remove = self.check_parameter(key=ParameterKeys.COLUMNS_TO_REMOVE_KEY, accepted_values=None, default_value=self.columns_to_remove)
        self.record_carrier_patients = self.check_parameter(key=ParameterKeys.RECORD_CARRIER_PATIENT, accepted_values=["True", "False", True, False], default_value=self.record_carrier_patients)
        self.patient_id_column_name = MetadataColumns.normalize_name(self.check_parameter(key=ParameterKeys.PATIENT_ID_COLUMN, accepted_values=None, default_value=self.patient_id_column_name))
        self.sample_id_column_name = MetadataColumns.normalize_name(self.check_parameter(key=ParameterKeys.SAMPLE_ID_COLUMN, accepted_values=None, default_value=self.patient_id_column_name))

        # create working files for the ETL
        self.create_current_working_dir()
        self.setup_logging_files()

    def file_set_up(self, setup_files: bool) -> None:
        log.info("in file_set_up()")

        # # D. set up the anonymized patient id data file
        # # this should NOT be merged with the setup of data files as this as to be set even though no data is provided
        # # (this happens in tests: data is given by hand, i.e., without set_up, but the anonymized patient IDs file still has to exist
        # self.anonymized_patient_ids_filepath = self.check_parameter(key=ParameterKeys.ANONYMIZED_PATIENT_IDS, accepted_values=None, default_value=self.anonymized_patient_ids_filepath)
        # log.debug(self.anonymized_patient_ids_filepath)
        # if self.anonymized_patient_ids_filepath is not None:
        #     # it may be None when we are computing the catalogue data (because we don't need it)
        self.setup_mapping_to_anonymized_patient_ids()

        # E. set up the data and metadata files
        if setup_files:
            log.debug("I will also set up data files")
            # 1. compute the (Docker-rooted) absolute path to the metadata file
            self.metadata_filepath = self.check_parameter(key=ParameterKeys.METADATA_PATH, accepted_values=None, default_value=self.metadata_filepath)
            log.debug(self.metadata_filepath)
            if os.sep in self.metadata_filepath:
                raise ValueError(f"The provided metadata file {self.metadata_filepath} should be only the name of the metadata file, but it looks like a path.")
            if os.getenv("CONTEXT_MODE") == "TEST":
                self.metadata_filepath = os.path.join(DOCKER_FOLDER_TEST, self.metadata_filepath)
            else:
                self.metadata_filepath = os.path.join(DOCKER_FOLDER_METADATA, self.metadata_filepath)

    def check_parameter(self, key: str, accepted_values: list|None, default_value) -> str | bool | int | None | list:
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
                int_parameter = cast_str_to_int(str_value=the_parameter)
                if int_parameter is not None:
                    return int_parameter
                else:
                    # trying to cast as list
                    if the_parameter.startswith("[") and the_parameter.endswith("]"):
                        the_parameter = the_parameter.replace("[", "").replace("]", "")
                        return the_parameter.split(",")
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
        execution_folder = os.path.join(working_dir_with_db, self.execution_date.isoformat())
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

    def setup_mapping_to_anonymized_patient_ids(self):
        # pid file should be located at in the DB folder, not the DB+date folder. this allows to:
        # - reuse the same PID file for a database that is consolidated (data is appended) several times
        # - while being sure to reuse the previous PIDs when consolidating the db
        # - we can also easily reset that PID file when db_drop==true (otherwise, we reuse the pid file)
        pid_filename = "anonymized_patient_ids.json"
        if os.getenv("CONTEXT_MODE") == "TEST":
            self.anonymized_patient_ids_filepath = os.path.join(DOCKER_FOLDER_TEST, pid_filename)
        else:
            self.anonymized_patient_ids_filepath = os.path.join(self.working_dir_current, "..", pid_filename)

        log.debug(self.anonymized_patient_ids_filepath)
        if not os.path.exists(self.anonymized_patient_ids_filepath) or os.stat(self.anonymized_patient_ids_filepath).st_size == 0 or self.db_drop is True:
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
        return {"my_exec": "exec"}
        # return dataclasses.asdict(self, dict_factory=factory)

    def __str__(self):
        return json.dumps(self.to_json())
