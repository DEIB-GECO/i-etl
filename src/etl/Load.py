import traceback

from config.BetterConfig import BetterConfig
from database.Database import Database
from utils.TableNames import TableNames
from utils.setup_logger import log


class Load:
    def __init__(self, database: Database, config: BetterConfig, create_indexes: bool):
        self.database = database
        self.config = config
        self.create_indexes = create_indexes

    def run(self) -> None:
        # Insert resources that have not been inserted yet, i.e.,
        # anything else than Hospital, Examination and Disease instances
        log.debug("in the Load class")
        self.database.load_json_in_table(table_name=TableNames.PATIENT.value, unique_variables=["identifier"])

        self.database.load_json_in_table(table_name=TableNames.EXAMINATION_RECORD.value, unique_variables=["recordedBy", "subject", "basedOn", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.SAMPLE.value, unique_variables=["identifier"])

        # if everything has been loaded, we can create indexes
        if self.create_indexes and not self.config.get_no_index():
            self.create_db_indexes()

    def create_db_indexes(self) -> None:
        log.info("Creating indexes.")
        # try:
        # 1. for each resource type, we create an index on its "identifier" and its creation date "createdAt"
        for table_name in TableNames:
            self.database.create_unique_index(table_name=table_name.value, columns={"identifier.value": 1})
            self.database.create_non_unique_index(table_name=table_name.value, columns={"createdAt": 1})
        # 2. next, we also create resource-wise indexes
        # for Examination instances, we create an index both on the ontology (system) and a code
        # this is because we usually ask for a code for a given ontology (what is a coe without its ontology? nothing)
        self.database.create_non_unique_index(table_name=TableNames.EXAMINATION.value, columns={"code.coding.system": 1, "code.coding.code": 1})
        # for ExaminationRecord instances, we create an index per reference because we usually join each reference to a table
        self.database.create_non_unique_index(table_name=TableNames.EXAMINATION_RECORD.value, columns={"instantiate.reference": 1})
        self.database.create_non_unique_index(table_name=TableNames.EXAMINATION_RECORD.value, columns={"subject.reference": 1})
        self.database.create_non_unique_index(table_name=TableNames.EXAMINATION_RECORD.value, columns={"basedOn.reference": 1})
        log.info("Finished to create indexes.")
