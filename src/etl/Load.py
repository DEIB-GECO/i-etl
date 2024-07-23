from database.Database import Database
from database.Execution import Execution
from enums.TableNames import TableNames
from utils.setup_logger import log


class Load:
    def __init__(self, database: Database, execution: Execution, create_indexes: bool):
        self.database = database
        self.execution = execution
        self.create_indexes = create_indexes

    def run(self) -> None:
        # Insert resources that have not been inserted yet, i.e.,
        # anything else than Hospital, Examination and Disease instances
        log.debug(f"in the Load class")
        self.load_remaining_data()

        # if everything has been loaded, we can create indexes
        if self.create_indexes and not self.execution.db_no_index:
            self.create_db_indexes()

    def load_remaining_data(self) -> None:
        self.database.load_json_in_table(table_name=TableNames.PATIENT, unique_variables=["identifier"])

        self.database.load_json_in_table(table_name=TableNames.LABORATORY_RECORD, unique_variables=["recorded_by", "subject", "based_on", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.SAMPLE, unique_variables=["identifier"])

    def create_db_indexes(self) -> None:
        log.info(f"Creating indexes.")
        # 1. for each resource type, we create an index on its "identifier" and its creation date "timestamp"
        for table_name in TableNames.values():
            self.database.create_unique_index(table_name=table_name, columns={"identifier.value": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"timestamp": 1})
        # 2. next, we also create resource-wise indexes
        # for Examination instances, we create an index both on the ontology (system) and a code
        # this is because we usually ask for a code for a given ontology (what is a coe without its ontology? nothing)
        self.database.create_non_unique_index(table_name=TableNames.LABORATORY_FEATURE, columns={"code.coding.system": 1, "code.coding.code": 1})
        # for ExaminationRecord instances, we create an index per reference because we usually join each reference to a table
        self.database.create_non_unique_index(table_name=TableNames.LABORATORY_RECORD, columns={"instantiate.reference": 1})
        self.database.create_non_unique_index(table_name=TableNames.LABORATORY_RECORD, columns={"subject.reference": 1})
        self.database.create_non_unique_index(table_name=TableNames.LABORATORY_RECORD, columns={"based_on.reference": 1})
        log.info(f"Finished to create indexes.")
