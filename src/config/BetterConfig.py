import configparser
import getpass
import os.path
import platform

from datetime import datetime

import pymongo

from src.utils.constants import DEFAULT_CONFIG_FILE
from utils.setup_logger import log


class BetterConfig:
    FILES_SECTION = "FILES"
    DB_SECTION = "DATABASE"
    HOSPITAL_SECTION = "HOSPITAL"
    SYSTEM_SECTION = "SYSTEM"

    WORKING_DIR_KEY = "working_dir"
    WORKING_DIR_CURRENT_KEY = "working_key_current"
    METADATA_FILEPATH_KEY = "metadata_filepath"
    DATA_FILEPATH_KEY = "data_filepath"
    CONNECTION_KEY = "connection"
    NAME_KEY = "name"
    DROP_KEY = "drop"
    PYTHON_VERSION_KEY = "python_version"
    PYMONGO_VERSION_KEY = "pymongo_version"
    EXECUTION_KEY = "execution_date"
    PLATFORM_KEY = "platform"
    PLATFORM_VERSION_KEY = "platform_version"
    USER_KEY = "user"

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(DEFAULT_CONFIG_FILE)
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

    def set_data_filepath(self, data_filepath):
        self.set_files_section()
        self.config.set(BetterConfig.FILES_SECTION, BetterConfig.DATA_FILEPATH_KEY, data_filepath)

    def set_db_connection(self, db_connection):
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.CONNECTION_KEY, db_connection)

    def set_db_name(self, db_name):
        self.set_database_section()
        self.config.set(BetterConfig.DB_SECTION, BetterConfig.NAME_KEY, db_name)
        log.debug(db_name)
        log.debug(self.config.has_option("DATABASE", "name"))
        log.debug(self.config.get("DATABASE", "name"))
        log.debug(self.to_json())

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

    def get_data_filepath(self):
        try:
            return self.config.get(BetterConfig.FILES_SECTION, BetterConfig.DATA_FILEPATH_KEY)
        except:
            return ""

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
            BetterConfig.FILES_SECTION + "/" + BetterConfig.DATA_FILEPATH_KEY: self.get_data_filepath(),
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
    