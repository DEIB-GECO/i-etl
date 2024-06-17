import configparser
import getpass
import os.path
import platform
import shutil

from datetime import datetime

import pymongo

from src.utils.constants import DEFAULT_CONFIG_FILE, DEFAULT_DB_NAME
from src.utils.setup_logger import log


class BetterConfig:
    FILES_SECTION = "FILES"
    DB_SECTION = "DATABASE"
    HOSPITAL_SECTION = "HOSPITAL"
    SYSTEM_SECTION = "SYSTEM"

    WORKING_DIR_KEY = "working_dir"
    WORKING_DIR_CURRENT_KEY = "working_key_current"
    METADATA_FILEPATH_KEY = "metadata_filepath"
    DATA_FILEPATHS_KEY = "data_filepaths"
    CURRENT_FILEPATH_KEY = "current_filepath"
    CONNECTION_KEY = "connection"
    NAME_KEY = "name"
    DROP_KEY = "drop"
    PYTHON_VERSION_KEY = "python_version"
    PYMONGO_VERSION_KEY = "pymongo_version"
    EXECUTION_KEY = "execution_date"
    PLATFORM_KEY = "platform"
    PLATFORM_VERSION_KEY = "platform_version"
    USER_KEY = "user"

    def __init__(self, args=None):
        self.config = configparser.ConfigParser()
        self.config.read(DEFAULT_CONFIG_FILE)
        log.debug(self.to_json())

        if args is not None:
            # the user gave parameters
            # set the Config internals with the user parameters (taken as Python main arguments)
            self.args = args
            self.set_from_parameters()
        else:
            # the user did not provide any parameters
            # or we are in test mode
            # in any case, we have read the default config, so we are good
            pass

    def set_from_parameters(self):
        if self.args.hospital_name is not None:
            self.set_hospital_name(self.args.hospital_name)
        if self.args.connection is not None:
            self.set_db_connection(self.args.connection)
        if self.args.database_name is not None and self.args.database_name != "":
            self.set_db_name(self.args.database_name)
        else:
            log.info("There was no database name provided. Using the default one: %s", DEFAULT_DB_NAME)
            self.set_db_name(DEFAULT_DB_NAME)
        if self.args.drop is not None:
            self.set_db_drop(self.args.drop)

        # create a new folder within the tmp dir to store the current execution tmp files and config
        # this folder is named after the DB name (instead of a timestamp, which will create one folder at each run)
        working_folder = os.path.join(self.get_working_dir(), self.get_db_name())
        self.set_working_dir_current(working_folder)
        if os.path.exists(working_folder):
            shutil.rmtree(working_folder)  # empty the current working directory if it exists
        os.makedirs(working_folder)  # create the working folder (labelled with the DB name)

        # get metadata and data filepaths
        if self.args.metadata_filepath is None:
            log.error("No metadata file path has been provided. Please provide one.")
            exit()
        elif not os.path.isfile(self.args.metadata_filepath):
            log.error("The specified metadata file does not seem to exist. Please check the path.")
            exit()
        else:
            metadata_filename = "metadata-" + self.args.hospital_name + ".csv"
            metadata_filepath = os.path.join(self.get_working_dir_current(), metadata_filename)
            shutil.copyfile(self.args.metadata_filepath, metadata_filepath)
            self.set_metadata_filepath(metadata_filepath)

        if self.args.data_filepath is None:
            log.error("No data file path has been provided. Please provide one.")
            exit()
        else:
            # if there is a single file, this will put that file in a list
            # otherwise, when the user provides several files, it will split them in the array
            log.debug(self.args.data_filepath)
            split_files = self.args.data_filepath.split(",")
            log.debug(split_files)
            for current_file in split_files:
                if not os.path.isfile(current_file):
                    log.error("The specified data file '%s' does not seem to exist. Please check the path.",
                              current_file)
                    exit()
            # we do not copy the data in our working dir because it is too large to be copied
            self.set_data_filepaths(self.args.data_filepath)  # file 1,file 2, ...,file N
            log.debug(self.get_data_filepaths())

        # write more information about the current run in the config
        self.add_python_version()
        self.add_pymongo_version()
        self.add_execution_date()
        self.add_platform()
        self.add_platform_version()
        self.add_user()

        # save the config file in the current working directory
        self.write_to_file()

        # print the main parameters of the current run
        log.info("Selected hospital name: %s", self.get_hospital_name())
        log.info("The database name is %s", self.get_db_name())
        log.info("The connection string is: %s", self.get_db_connection())
        log.info("The database will be dropped: %s", self.get_db_drop())
        log.info("The metadata file is located at: %s", self.get_metadata_filepath())
        log.info("The data file is located at: %s", self.get_data_filepaths())
        log.debug(self.to_json())

    # below, define methods for each parameter in the config
    # keep it up-to-date wrt the config file
    def set_working_dir(self, working_dir):
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_KEY, working_dir)

    def set_working_dir_current(self, working_dir_current):
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_CURRENT_KEY, working_dir_current)

    def set_metadata_filepath(self, metadata_filepath):
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.METADATA_FILEPATH_KEY, metadata_filepath)

    def set_current_filepath(self, current_filepath: str):
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.CURRENT_FILEPATH_KEY, current_filepath)

    def set_data_filepaths(self, data_filepaths: str):
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.DATA_FILEPATHS_KEY, data_filepaths)

    def set_db_connection(self, db_connection):
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.CONNECTION_KEY, db_connection)

    def set_db_name(self, db_name):
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.NAME_KEY, db_name)

    def set_db_drop(self, drop):
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.DROP_KEY, drop)

    def set_hospital_name(self, hospital_name):
        self.set_hospital_section()
        self.config.set(BetterConfig.HOSPITAL_SECTION, BetterConfig.NAME_KEY, hospital_name)

    def add_python_version(self):
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PYTHON_VERSION_KEY, platform.python_version())

    def add_pymongo_version(self):
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PYMONGO_VERSION_KEY, pymongo.version)

    def add_execution_date(self):
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.EXECUTION_KEY, str(datetime.now()))

    def add_platform(self):
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_KEY, platform.platform())

    def add_platform_version(self):
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_VERSION_KEY, platform.version())

    def add_user(self):
        self.set_system_section()
        self.config.set(BetterConfig.SYSTEM_SECTION, BetterConfig.USER_KEY, getpass.getuser())

    # set sections
    def set_files_section(self):
        if not self.config.has_section(BetterConfig.FILES_SECTION):
            self.config.add_section(BetterConfig.FILES_SECTION)

    def set_database_section(self):
        if not self.config.has_section(BetterConfig.DB_SECTION):
            self.config.add_section(BetterConfig.DB_SECTION)

    def set_hospital_section(self):
        if not self.config.has_section(BetterConfig.HOSPITAL_SECTION):
            self.config.add_section(BetterConfig.HOSPITAL_SECTION)

    def set_system_section(self):
        if not self.config.has_section(BetterConfig.SYSTEM_SECTION):
            self.config.add_section(BetterConfig.SYSTEM_SECTION)

    # get config variables
    def get_working_dir(self):
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_KEY)
        except:
            return ""

    def get_working_dir_current(self):
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.WORKING_DIR_CURRENT_KEY)
        except:
            return ""

    def get_metadata_filepath(self):
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.METADATA_FILEPATH_KEY)
        except:
            return ""

    def get_current_filepath(self):
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.CURRENT_FILEPATH_KEY)
        except:
            return ""

    def get_data_filepaths(self) -> list:
        try:
            # return the list of files instead of the stringified list of files
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.DATA_FILEPATHS_KEY).split(",")
        except:
            return []

    def get_db_connection(self):
        try:
            return self.config.get(BetterConfig.DB_SECTION, BetterConfig.CONNECTION_KEY)
        except:
            return ""

    def get_db_name(self):
        try:
            return self.config.get(BetterConfig.DB_SECTION, BetterConfig.NAME_KEY)
        except:
            return ""

    def get_db_drop(self) -> bool:
        try:
            return self.config.get(BetterConfig.DB_SECTION, BetterConfig.DROP_KEY) == "True"
        except:
            return False

    def get_hospital_name(self):
        try:
            return self.config.get(BetterConfig.HOSPITAL_SECTION, BetterConfig.NAME_KEY)
        except:
            return ""

    def get_python_version(self):
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PYTHON_VERSION_KEY)
        except:
            return ""

    def get_pymongo_version(self):
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PYMONGO_VERSION_KEY)
        except:
            return ""

    def get_execution_date(self):
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.EXECUTION_KEY)
        except:
            return ""

    def get_platform(self):
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_KEY)
        except:
            return ""

    def get_platform_version(self):
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.PLATFORM_VERSION_KEY)
        except:
            return ""

    def get_user(self):
        try:
            return self.config.get(BetterConfig.SYSTEM_SECTION, BetterConfig.USER_KEY)
        except:
            return ""

    # write config to file
    def write_to_file(self):
        config_filepath = os.path.join(self.get_working_dir_current(), DEFAULT_CONFIG_FILE)
        with open(config_filepath, 'w') as f:
            self.config.write(f)

    def to_json(self):
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
    