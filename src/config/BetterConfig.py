import configparser
import getpass
import os.path
import platform
import shutil

from datetime import datetime

import pymongo
from argparse import Namespace

from src.utils.constants import DEFAULT_CONFIG_FILE, DEFAULT_DB_NAME
from src.utils.setup_logger import log


class BetterConfig:
    FILES_SECTION = "FILES"
    DB_SECTION = "DATABASE"
    HOSPITAL_SECTION = "HOSPITAL"
    SYSTEM_SECTION = "SYSTEM"
    RUN_SECTION = "RUN"

    # FILES section
    WORKING_DIR_KEY = "working_dir"
    WORKING_DIR_CURRENT_KEY = "working_key_current"
    METADATA_FILEPATH_KEY = "metadata_filepath"
    DATA_FILEPATHS_KEY = "data_filepaths"
    CURRENT_FILEPATH_KEY = "current_filepath"
    # DATABASE section
    CONNECTION_KEY = "connection"
    DROP_KEY = "drop"
    # DATABASE and HOSPITAL sections
    NAME_KEY = "name"
    # SYSTEM section
    PYTHON_VERSION_KEY = "python_version"
    PYMONGO_VERSION_KEY = "pymongo_version"
    EXECUTION_KEY = "execution_date"
    PLATFORM_KEY = "platform"
    PLATFORM_VERSION_KEY = "platform_version"
    USER_KEY = "user"
    USE_EN_LOCALE_KEY = "locale"
    # RUN section
    EXTRACT_KEY = "extract"
    TRANSFORM_KEY = "transform"
    LOAD_KEY = "load"
    ANALYSIS_KEY = "analysis"

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(DEFAULT_CONFIG_FILE)
        log.debug(self.to_json())

    def set_from_parameters(self, args: Namespace) -> None:
        # the user gave parameters
        # set the Config internals with the user parameters (taken as Python main arguments)
        # otherwise (when no parameters are provided), the default config is used
        self.set_hospital_name(hospital_name=args.hospital_name)
        self.set_db_connection(db_connection=args.connection)
        self.set_db_name(db_name=args.database_name)
        self.set_db_drop(drop=args.drop)

        # create a new folder within the tmp dir to store the current execution tmp files and config
        # this folder is named after the DB name (instead of a timestamp, which will create one folder at each run)
        working_folder = os.path.join(self.get_working_dir(), self.get_db_name())
        self.set_working_dir_current(working_dir_current=working_folder)
        if os.path.exists(working_folder):
            shutil.rmtree(working_folder)  # empty the current working directory if it exists
        os.makedirs(working_folder)  # create the working folder (labelled with the DB name)

        # get metadata and data filepaths
        if not os.path.isfile(args.metadata_filepath):
            log.error("The specified metadata file does not seem to exist. Please check the path.")
            exit()
        else:
            metadata_filename = "metadata-" + args.hospital_name + ".csv"
            metadata_filepath = os.path.join(self.get_working_dir_current(), metadata_filename)
            shutil.copyfile(args.metadata_filepath, metadata_filepath)
            self.set_metadata_filepath(metadata_filepath=metadata_filepath)

        # if there is a single file, this will put that file in a list
        # otherwise, when the user provides several files, it will split them in the array
        log.debug(args.data_filepath)
        split_files = args.data_filepath.split(",")
        log.debug(split_files)
        for current_file in split_files:
            if not os.path.isfile(current_file):
                log.error("The specified data file '%s' does not seem to exist. Please check the path.",
                          current_file)
                exit()
        # we do not copy the data in our working dir because it is too large to be copied
        self.set_data_filepaths(data_filepaths=args.data_filepath)  # file 1,file 2, ...,file N
        log.debug(self.get_data_filepaths())

        # write more information about the current run in the config
        self.add_python_version()
        self.add_pymongo_version()
        self.add_execution_date()
        self.add_platform()
        self.add_platform_version()
        self.add_user()
        self.set_use_en_locale(use_en_locale=args.use_en_locale)

        # and about the user parameters
        log.debug("self.args.extract = %s", args.extract)
        self.set_extract(extract=args.extract)
        self.set_transform(transform=args.transform)
        self.set_load(load=args.load)
        self.set_analysis(analyze=args.analysis)

        # save the config file in the current working directory
        self.write_to_file()

        # print the main parameters of the current run
        log.info("The hospital name is: %s", self.get_hospital_name())
        log.info("The database name is %s", self.get_db_name())
        log.info("The database will be dropped: %s", ("yes" if self.get_db_drop() else "no"))
        log.info("The connection string is: %s", self.get_db_connection())
        log.info("The database will be dropped: %s", self.get_db_drop())
        log.info("The metadata file is located at: %s", self.get_metadata_filepath())
        log.info("The data files are located at: %s", self.get_data_filepaths())
        log.info("The data files are located at: %s", self.get_data_filepaths())
        log.info("The Extract step will be performed: %s", ("yes" if self.get_extract() else "no"))
        log.info("The Analysis step will be performed: %s", ("yes" if self.get_analysis() else "no"))
        log.info("The Transform step will be performed: %s", ("yes" if self.get_transform() else "no"))
        log.info("The Load step will be performed: %s", ("yes" if self.get_load() else "no"))
        log.info("Use english (en_US) locale instead of the one assigned by the system: %s", ("yes" if self.get_use_en_locale() else "no"))
        log.debug(self.to_json())

    # below, define methods for each parameter in the config
    # keep it up-to-date wrt the config file
    def set_working_dir(self, working_dir: str) -> None:
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_KEY, working_dir)

    def set_working_dir_current(self, working_dir_current: str) -> None:
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_CURRENT_KEY, working_dir_current)

    def set_metadata_filepath(self, metadata_filepath: str) -> None:
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.METADATA_FILEPATH_KEY, metadata_filepath)

    def set_current_filepath(self, current_filepath: str) -> None:
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.CURRENT_FILEPATH_KEY, current_filepath)

    def set_data_filepaths(self, data_filepaths: str) -> None:
        # data_filepaths is a set of data filepaths, concatenated with commas (,)
        # this is what we get from the user input parameters
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.DATA_FILEPATHS_KEY, data_filepaths)

    def set_db_connection(self, db_connection: str) -> None:
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.CONNECTION_KEY, db_connection)

    def set_db_name(self, db_name: str) -> None:
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.NAME_KEY, db_name)

    def set_db_drop(self, drop: str) -> None:
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.DROP_KEY, drop)

    def set_hospital_name(self, hospital_name: str) -> None:
        self.set_hospital_section()
        self.config.set(BetterConfig.HOSPITAL_SECTION, BetterConfig.NAME_KEY, hospital_name)

    def add_python_version(self) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PYTHON_VERSION_KEY, platform.python_version())

    def add_pymongo_version(self) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PYMONGO_VERSION_KEY, pymongo.version)

    def add_execution_date(self) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.EXECUTION_KEY, str(datetime.now()))

    def add_platform(self) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_KEY, platform.platform())

    def add_platform_version(self) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_VERSION_KEY, platform.version())

    def add_user(self) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.USER_KEY, getpass.getuser())

    def set_use_en_locale(self, use_en_locale: str) -> None:
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.USE_EN_LOCALE_KEY, use_en_locale)

    def set_extract(self, extract: str) -> None:
        self.set_run_section()
        self.config.set(BetterConfig.RUN_SECTION, BetterConfig.EXTRACT_KEY, extract)

    def set_transform(self, transform: str) -> None:
        self.set_run_section()
        self.config.set(BetterConfig.RUN_SECTION, BetterConfig.TRANSFORM_KEY, transform)

    def set_load(self, load: str) -> None:
        self.set_run_section()
        self.config.set(BetterConfig.RUN_SECTION, BetterConfig.LOAD_KEY, load)

    def set_analysis(self, analyze: str) -> None:
        self.set_run_section()
        self.config.set(BetterConfig.RUN_SECTION, BetterConfig.ANALYSIS_KEY, analyze)

    # set sections
    def set_files_section(self) -> None:
        if not self.config.has_section(BetterConfig.FILES_SECTION):
            self.config.add_section(BetterConfig.FILES_SECTION)

    def set_database_section(self) -> None:
        if not self.config.has_section(BetterConfig.DB_SECTION):
            self.config.add_section(BetterConfig.DB_SECTION)

    def set_hospital_section(self) -> None:
        if not self.config.has_section(BetterConfig.HOSPITAL_SECTION):
            self.config.add_section(BetterConfig.HOSPITAL_SECTION)

    def set_system_section(self) -> None:
        if not self.config.has_section(BetterConfig.SYSTEM_SECTION):
            self.config.add_section(BetterConfig.SYSTEM_SECTION)

    def set_run_section(self) -> None:
        if not self.config.has_section(BetterConfig.RUN_SECTION):
            self.config.add_section(BetterConfig.RUN_SECTION)

    # get config variables
    def get_working_dir(self) -> str:
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_KEY)
        except Exception:
            return ""

    def get_working_dir_current(self) -> str:
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_CURRENT_KEY)
        except Exception:
            return ""

    def get_metadata_filepath(self) -> str:
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.METADATA_FILEPATH_KEY)
        except Exception:
            return ""

    def get_current_filepath(self) -> str:
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.CURRENT_FILEPATH_KEY)
        except Exception:
            return ""

    def get_data_filepaths(self) -> list:
        try:
            # return the list of files instead of the stringified list of files
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.DATA_FILEPATHS_KEY).split(",")
        except Exception:
            return []

    def get_db_connection(self) -> str:
        try:
            return self.config.get(BetterConfig.DB_SECTION, BetterConfig.CONNECTION_KEY)
        except Exception:
            return ""

    def get_db_name(self) -> str:
        try:
            return self.config.get(BetterConfig.DB_SECTION, BetterConfig.NAME_KEY)
        except Exception:
            return ""

    def get_db_drop(self) -> bool:
        try:
            return self.config.get(BetterConfig.DB_SECTION, BetterConfig.DROP_KEY) == "True"
        except Exception:
            return False

    def get_hospital_name(self) -> str:
        try:
            return self.config.get(BetterConfig.HOSPITAL_SECTION, BetterConfig.NAME_KEY)
        except Exception:
            return ""

    def get_python_version(self) -> str:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PYTHON_VERSION_KEY)
        except Exception:
            return ""

    def get_pymongo_version(self) -> str:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PYMONGO_VERSION_KEY)
        except Exception:
            return ""

    def get_execution_date(self) -> str:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.EXECUTION_KEY)
        except Exception:
            return ""

    def get_platform(self) -> str:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_KEY)
        except Exception:
            return ""

    def get_platform_version(self) -> str:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_VERSION_KEY)
        except Exception:
            return ""

    def get_user(self) -> str:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.USER_KEY)
        except Exception:
            return ""

    def get_use_en_locale(self) -> bool:
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.USE_EN_LOCALE_KEY) == "True"
        except Exception:
            return False

    def get_extract(self) -> bool:
        try:
            return self.config.get(BetterConfig.RUN_SECTION, BetterConfig.EXTRACT_KEY) == "True"
        except Exception:
            return False

    def get_transform(self) -> bool:
        try:
            return self.config.get(BetterConfig.RUN_SECTION, BetterConfig.TRANSFORM_KEY) == "True"
        except Exception:
            return False

    def get_load(self) -> bool:
        try:
            return self.config.get(BetterConfig.RUN_SECTION, BetterConfig.LOAD_KEY) == "True"
        except Exception:
            return False

    def get_analysis(self) -> bool:
        try:
            return self.config.get(BetterConfig.RUN_SECTION, BetterConfig.ANALYSIS_KEY) == "True"
        except Exception:
            return False

    # write config to file
    def write_to_file(self) -> None:
        config_filepath = os.path.join(self.get_working_dir_current(), DEFAULT_CONFIG_FILE)
        with open(config_filepath, 'w') as f:
            self.config.write(f)

    def to_json(self) -> dict:
        return {
            BetterConfig.FILES_SECTION + "/" + BetterConfig.WORKING_DIR_KEY: self.get_working_dir(),
            BetterConfig.FILES_SECTION + "/" + BetterConfig.WORKING_DIR_CURRENT_KEY: self.get_working_dir_current(),
            BetterConfig.FILES_SECTION + "/" + BetterConfig.METADATA_FILEPATH_KEY: self.get_metadata_filepath(),
            BetterConfig.FILES_SECTION + "/" + BetterConfig.DATA_FILEPATHS_KEY: self.get_data_filepaths(),
            BetterConfig.FILES_SECTION + "/" + BetterConfig.CURRENT_FILEPATH_KEY: self.get_current_filepath(),
            BetterConfig.DB_SECTION + "/" + BetterConfig.CONNECTION_KEY: self.get_db_connection(),
            BetterConfig.DB_SECTION + "/" + BetterConfig.NAME_KEY: self.get_db_name(),
            BetterConfig.DB_SECTION + "/" + BetterConfig.DROP_KEY: self.get_db_drop(),
            BetterConfig.HOSPITAL_SECTION + "/" + BetterConfig.NAME_KEY: self.get_hospital_name(),
            BetterConfig.SYSTEM_SECTION + "/" + BetterConfig.PYTHON_VERSION_KEY: self.get_python_version(),
            BetterConfig.SYSTEM_SECTION + "/" + BetterConfig.EXECUTION_KEY: self.get_execution_date(),
            BetterConfig.SYSTEM_SECTION + "/" + BetterConfig.PLATFORM_KEY: self.get_platform(),
            BetterConfig.SYSTEM_SECTION + "/" + BetterConfig.PLATFORM_VERSION_KEY: self.get_platform_version(),
            BetterConfig.SYSTEM_SECTION + "/" + BetterConfig.USER_KEY: self.get_user()
        }
    