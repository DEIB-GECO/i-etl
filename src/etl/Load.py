from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log


class Load:
    def __init__(self, database: Database, config: BetterConfig):
        self.database = database
        self.config = config

    def run(self) -> None:
        # Insert resources that have not been inserted yet, i.e.,
        # anything else than Hospital, Examination and Disease instances
        log.debug("in the Load class")
        self.database.load_json_in_table(table_name=TableNames.PATIENT.value, unique_variables=["identifier"])

        self.database.load_json_in_table(table_name=TableNames.EXAMINATION_RECORD.value, unique_variables=["recordedBy", "subject", "basedOn", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.SAMPLE.value, unique_variables=["identifier"])

    def create_db_indexes(self) -> None:
        self.database.create_unique_index(table_name=TableNames.PATIENT.value, columns={"metadata.csv_filepath": 1, "metadata.csv_line": 1})
        self.database.create_unique_index(table_name=TableNames.HOSPITAL.value, columns={"id": 1, "name": 1})
        self.database.create_unique_index(table_name=TableNames.EXAMINATION.value, columns={"code.codings.system": 1, "code.codings.code": 1})
        self.database.create_unique_index(table_name=TableNames.EXAMINATION_RECORD.value, columns={"instantiate.reference": 1, "subject.reference": 1, "recorded_by.reference": 1, "value": 1, "issued": 1})
