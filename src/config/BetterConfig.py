import configparser

from src.utils.constants import DEFAULT_CONFIG_FILE


class BetterConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(DEFAULT_CONFIG_FILE)

    # below, define methods for each parameter in the config
    # keep it up-to-date wrt the config file
    def set_working_dir(self, working_dir):
        self.set_files_section()
        self.config.set("FILES", "working_dir", working_dir)

    def set_working_dir_current(self, working_dir_current):
        self.set_files_section()
        self.config.set("FILES", "working_dir_current", working_dir_current)

    def set_metadata_filepath(self, metadata_filepath):
        self.set_files_section()
        self.config.set("FILES", "metadata_filepath", metadata_filepath)

    def set_data_filepath(self, data_filepath):
        self.set_files_section()
        self.config.set("FILES", "data_filepath", data_filepath)

    def set_db_connection(self, db_connection):
        self.set_database_section()
        self.config.set("DATABASE", "connection", db_connection)

    def set_db_name(self, db_name):
        self.set_database_section()
        self.config.set("DATABASE", "name", db_name)

    def set_db_reset(self, reset):
        self.set_database_section()
        self.config.set("DATABASE", "reset", reset)

    def set_hospital_name(self, hospital_name):
        self.set_hospital_section()
        self.config.set("HOSPITAL", "name", hospital_name)

    def set_python_version(self, python_version):
        self.set_system_section()
        self.config.set("SYSTEM", "python_version", python_version)

    def set_execution_date(self, execution_date):
        self.set_system_section()
        self.config.set("SYSTEM", "execution_date", execution_date)

    def set_platform(self, platform):
        self.set_system_section()
        self.config.set("SYSTEM", "platform", platform)

    def set_platform_version(self, platform_version):
        self.set_system_section()
        self.config.set("SYSTEM", "platform_version", platform_version)

    def set_user(self, user):
        self.set_system_section()
        self.config.set("SYSTEM", "user", user)

    # set sections
    def set_files_section(self):
        if not self.config.has_section("FILES"):
            self.config.add_section("FILES")

    def set_database_section(self):
        if not self.config.has_section("DATABASE"):
            self.config.add_section("DATABASE")

    def set_hospital_section(self):
        if not self.config.has_section("HOSPITAL"):
            self.config.add_section("HOSPITAL")

    def set_system_section(self):
        if not self.config.has_section("SYSTEM"):
            self.config.add_section("SYSTEM")

    # get config variables
    def get_working_dir(self):
        return self.config.get("FILES", "working_dir")

    def get_working_dir_current(self):
        return self.config.get("FILES", "working_dir_current")

    def get_metadata_filepath(self):
        return self.config.get("FILES", "metadata_filepath")

    def get_data_filepath(self):
        return self.config.get("FILES", "data_filepath")

    def get_db_connection(self):
        return self.config.get("DATABASE", "connection")

    def get_db_name(self):
        return self.config.get("DATABASE", "name")

    def get_db_reset(self):
        return self.config.get("DATABASE", "reset")

    def get_hospital_name(self):
        return self.config.get("HOSPITAL", "name")

    def get_python_version(self):
        return self.config.get("SYSTEM", "python_version")

    def get_execution_date(self):
        return self.config.get("SYSTEM", "execution_date")

    def get_platform(self):
        return self.config.get("SYSTEM", "platform")

    def get_platform_version(self):
        return self.config.get("SYSTEM", "platform_version")

    def get_user(self):
        return self.config.get("SYSTEM", "user")

    # write config to file
    def write_to_file(self, config_filepath):
        with open(config_filepath, 'w') as f:
            self.config.write(f)
