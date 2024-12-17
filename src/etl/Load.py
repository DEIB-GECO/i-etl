from database.Database import Database
from database.Execution import Execution
from enums.Profile import Profile
from enums.TableNames import TableNames
from etl.Task import Task
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.setup_logger import log


class Load(Task):
    def __init__(self, database: Database, execution: Execution, create_indexes: bool,
                 dataset_number: int, profile: str,
                 quality_stats: QualityStatistics, time_stats: TimeStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats, time_stats=time_stats)
        self.create_indexes = create_indexes
        self.dataset_number = dataset_number
        self.profile = profile

    def run(self) -> None:
        # Insert resources that have not been inserted yet, i.e., all Record instances
        log.debug(f"in the Load class")
        self.load_records()

        # if everything has been loaded, we can create indexes
        if self.create_indexes:
            self.create_db_indexes()

    def load_records(self) -> None:
        log.info(f"load {self.profile} records")
        unique_variables = ["registered_by", "has_subject", "instantiates", "dataset"]
        if self.profile == Profile.DIAGNOSIS:
            # we allow patients to have several diagnoses
            unique_variables.append("value")
        self.database.load_json_in_table(profile=self.profile, table_name=TableNames.RECORD, unique_variables=unique_variables, dataset_number=self.dataset_number)

    def create_db_indexes(self) -> None:
        log.info(f"Creating indexes.")

        count = 0

        # 1. for each resource type, we create an index on its "identifier" and its creation date "timestamp"
        for table_name in TableNames.data_tables():
            log.info(f"add index on id + timestamp + entity_type for table {table_name}")
            self.database.create_unique_index(table_name=table_name, columns={"identifier": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"timestamp": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"entity_type": 1})
            count += 3

        # 2. next, we also create resource-wise indexes

        # for Feature instances, we create an index both on the ontology (system) and a code
        # this is because we usually ask for a code for a given ontology (what is a code without its ontology? nothing)
        self.database.create_non_unique_index(table_name=TableNames.FEATURE, columns={"ontology_resource.system": 1, "ontology_resource.code": 1})
        count += 1

        # for Record instances, we create an index per reference because we usually join each reference to a table
        self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={"instantiates": 1})
        self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={"has_subject": 1})
        self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={"dataset": 1})
        count += 3

        # for Dataset entity only, we create an index on the global identifier
        self.database.create_unique_index(table_name=TableNames.DATASET, columns={"global_identifier": 1})
        log.info(f"Finished to create {count} indexes.")
