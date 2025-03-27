from database.Database import Database
from database.Execution import Execution
from entities.Record import Record
from entities.Resource import Resource
from enums.DiagnosisColumns import DiagnosisColumns
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.TimerKeys import TimerKeys
from etl.Task import Task
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.setup_logger import log


class Load(Task):
    def __init__(self, database: Database, execution: Execution, create_indexes: bool,
                 dataset_id: int, profile: str, quality_stats: QualityStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats)
        self.create_indexes = create_indexes
        self.dataset_id = dataset_id
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
        # we need to have registered_by, has_subject and instantiates for sure
        # we also need entity_type because we cannot have two indexes, one for non-clinical (reg, subj, inst) and one for clinical (reg, subj, inst, bid)
        # we also need base_id for the same reason, the value will be null for non-clinical records and clinical records without sample information
        unique_variables = [Record.REG_BY_, Record.SUBJECT_, Record.INSTANTIATES_, Resource.ENTITY_TYPE_, Record.BASE_ID_]
        if self.profile == Profile.DIAGNOSIS:
            # we allow patients to have several diagnoses
            unique_variables.append(DiagnosisColumns.DISEASE_COUNTER)
        log.info(unique_variables)
        self.database.load_json_in_table(table_name=TableNames.RECORD, unique_variables=unique_variables, dataset_id=self.dataset_id)

    def create_db_indexes(self) -> None:
        log.info(f"Creating indexes.")

        count = 0

        # 1. for each resource type, we create an index on its "identifier" and its creation date "timestamp"
        for table_name in TableNames.data_tables():
            log.info(f"add index on id + timestamp + entity_type for table {table_name}")
            self.database.create_unique_index(table_name=table_name, columns={Resource.IDENTIFIER_: 1})
            self.database.create_non_unique_index(table_name=table_name, columns={Resource.TIMESTAMP_: 1})
            self.database.create_non_unique_index(table_name=table_name, columns={Resource.ENTITY_TYPE_: 1})
            count += 3

        # 2. next, we also create resource-wise indexes

        # for Feature instances, we create an index both on the ontology (system) and a code
        # this is because we usually ask for a code for a given ontology (what is a code without its ontology? nothing)
        self.database.create_non_unique_index(table_name=TableNames.FEATURE, columns={"ontology_resource.system": 1, "ontology_resource.code": 1})
        count += 1

        # for Record instances, we create an index per reference because we usually join each reference to a table
        self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={Record.INSTANTIATES_: 1})
        self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={Record.SUBJECT_: 1})
        self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={Record.DATASET_: 1})
        count += 3
        # we cannot create an index on the base id because some records have it (the clinical ones)
        # while others do not have (imaging, phenotypic, etc.)
        # if has_base_id:
        #     self.database.create_non_unique_index(table_name=TableNames.RECORD, columns={"base_id": 1})
        #     count += 1

        log.info(f"Finished to create {count} indexes.")
