import json

from database.Database import Database
from database.Execution import Execution
from enums.TableNames import TableNames
from etl.Task import Task
from statistics.DatabaseStatistics import DatabaseStatistics
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.setup_logger import log


class Reporting(Task):

    NB_INSTANCES = "Number of instances"
    REC_NO_VALUE = "Record instances with no 'value' field"
    REC_NO_VALUE_INST = "Record instances with no 'value' field per 'instantiate'"
    CC_EMPTY_TEXT = "CodeableConcept with empty 'text' field"
    CC_EMPTY_LIST = "CodeableConcept with empty 'list' field"

    def __init__(self, database: Database, execution: Execution, quality_stats: QualityStatistics, db_stats: DatabaseStatistics,
                 time_stats: TimeStatistics):
        super().__init__(database=database, execution=execution, quality_stats=QualityStatistics(record_stats=False))
        self.quality_stats = quality_stats
        self.db_stats = db_stats
        self.time_stats = time_stats
        log.info(self.time_stats)

        # to get a user-friendly report
        self.report = {}

        # counts over the database after the ETL has finished
        self.counts_instances = {}
        self.nb_of_rec_with_no_value = {}
        self.nb_of_rec_with_no_value_per_instantiate = {}
        self.nb_of_cc_with_no_text_per_table = {}
        self.nb_of_cc_with_no_onto_resources_per_table = {}

    def run(self):
        self.report = {}
        self.report = self.report | self.db_stats.to_json()
        self.report = self.report | self.quality_stats.to_json()
        self.report = self.report | self.time_stats.to_json()

        # 4. print the final report
        # self.print_report()

        # 5. save each stat report in the database
        self.database.drop_table(table_name=TableNames.STATS_TIME)
        self.database.insert_one_tuple(table_name=TableNames.STATS_DB, one_tuple=self.db_stats.to_json())
        self.database.insert_one_tuple(table_name=TableNames.STATS_TIME, one_tuple=self.time_stats.to_json())
        self.database.insert_one_tuple(table_name=TableNames.STATS_QUALITY, one_tuple=self.quality_stats.to_json())

    def print_report(self) -> None:
        log.info("**** FINAL REPORT ****")
        # to get double-quoted keys and check it in jsonlint
        # we use default=str to be able to serialize datetime (otherwise non-serializable object)
        log.info(json.dumps(self.report, default=str))
