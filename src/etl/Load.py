from database.Database import Database
from database.Execution import Execution
from enums.TableNames import TableNames
from etl.Task import Task
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.setup_logger import log


class Load(Task):
    def __init__(self, database: Database, execution: Execution, create_indexes: bool, quality_stats: QualityStatistics, time_stats: TimeStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats, time_stats=time_stats)
        self.create_indexes = create_indexes

    def run(self) -> None:
        # Insert resources that have not been inserted yet, i.e.,
        # anything else than Hospital, LabFeature and Disease instances
        log.debug(f"in the Load class")
        self.load_remaining_data()

        # if everything has been loaded, we can create indexes
        if self.create_indexes:
            self.create_db_indexes()

    def load_remaining_data(self) -> None:
        self.database.load_json_in_table(table_name=TableNames.PATIENT, unique_variables=["identifier"])

        self.database.load_json_in_table(table_name=TableNames.LABORATORY_RECORD, unique_variables=["recorded_by", "subject", "based_on", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.SAMPLE, unique_variables=["identifier"])

        self.database.load_json_in_table(table_name=TableNames.DIAGNOSIS_RECORD, unique_variables=["recorded_by", "subject", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.MEDICINE_RECORD, unique_variables=["recorded_by", "subject", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.IMAGING_RECORD, unique_variables=["recorded_by", "subject", "instantiate"])

        self.database.load_json_in_table(table_name=TableNames.GENOMIC_RECORD, unique_variables=["recorded_by", "subject", "instantiate"])

    def create_db_indexes(self) -> None:
        log.info(f"Creating indexes.")

        count = 0

        # 1. for each resource type, we create an index on its "identifier" and its creation date "timestamp"
        for table_name in TableNames.values(db=self.database):
            self.database.create_unique_index(table_name=table_name, columns={"identifier": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"timestamp": 1})
            count += 2

        # 2. next, we also create resource-wise indexes

        # for Feature instances, we create an index both on the ontology (system) and a code
        # this is because we usually ask for a code for a given ontology (what is a coe without its ontology? nothing)
        for table_name in TableNames.features(db=self.database):
            # a table name of the form XFeature
            self.database.create_non_unique_index(table_name=table_name, columns={"code.coding.system": 1, "code.coding.code": 1})
            count += 1

        # for Record instances, we create an index per reference because we usually join each reference to a table
        for table_name in TableNames.records(db=self.database):
            # a table name of the form XRecord
            self.database.create_non_unique_index(table_name=table_name, columns={"instantiate.reference": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"subject.reference": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"based_on.reference": 1})
            count += 3

        log.info(f"Finished to create {count} indexes.")
