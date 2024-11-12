from database.Database import Database
from database.Execution import Execution
from enums.Profile import Profile
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
        log.info("load remaining data")
        record_table_name = Profile.get_record_table_name_from_profile(self.execution.current_file_profile)
        unique_variables = ["registered_by", "has_subject", "instantiates"]
        if self.execution.current_file_profile == Profile.DIAGNOSIS:
            # we allow patients to have several diagnoses
            unique_variables.append("value")
        self.database.load_json_in_table(table_name=record_table_name, unique_variables=unique_variables)

    def create_db_indexes(self) -> None:
        log.info(f"Creating indexes.")
        log.info(f"features tables: {TableNames.features(db=self.database)}")
        log.info(f"records tables: {TableNames.records(db=self.database)}")
        log.info(f"values tables: {TableNames.values(db=self.database)}")

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
            self.database.create_non_unique_index(table_name=table_name, columns={"ontology_resource.system": 1, "ontology_resource.code": 1})
            count += 1

        # for Record instances, we create an index per reference because we usually join each reference to a table
        for table_name in TableNames.records(db=self.database):
            # a table name of the form XRecord
            self.database.create_non_unique_index(table_name=table_name, columns={"instantiates": 1})
            self.database.create_non_unique_index(table_name=table_name, columns={"has_subject": 1})
            count += 2

        log.info(f"Finished to create {count} indexes.")
