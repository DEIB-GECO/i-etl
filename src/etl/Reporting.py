from database.Database import Database
from database.Execution import Execution
from enums.TableNames import TableNames
from etl.Task import Task
from utils.setup_logger import log


class Reporting(Task):

    NB_INSTANCES = "Number of instances"
    REC_NO_VALUE = "Record instances with no 'value' field"
    REC_NO_VALUE_INST = "Record instances with no 'value' field per 'instantiate'"
    CC_NO_TEXT = "CodeableConcept with empty 'text' field"
    CC_NO_CODING = "CodeableConcept with empty 'coding' field"

    def __init__(self, database: Database, execution: Execution):
        super().__init__(database, execution)

        self.counts_instances = {}
        self.nb_of_rec_with_no_value = {}
        self.nb_of_rec_with_no_value_per_instantiate = {}
        self.nb_of_cc_with_no_text_per_table = {}
        self.nb_of_cc_with_no_coding_per_table = {}

        # to get a user-friendly report per table
        self.report_per_table = {}

    def run(self):
        # 1. compute each parameter to report (a dict per parameter, each dict contains the values for the different tables
        self.counts_instances = self.get_counts_instances()
        log.info(self.counts_instances)
        self.nb_of_rec_with_no_value = self.get_nb_of_rec_with_no_value()
        log.info(self.nb_of_rec_with_no_value)
        self.nb_of_rec_with_no_value_per_instantiate = self.get_nb_of_rec_with_no_value_per_instantiate()
        log.info(self.nb_of_rec_with_no_value_per_instantiate)
        self.nb_of_cc_with_no_text_per_table = self.get_nb_of_cc_with_no_text_per_table()
        log.info(self.nb_of_cc_with_no_text_per_table)
        self.nb_of_cc_with_no_coding_per_table = self.get_nb_of_cc_with_no_coding_per_table()
        log.info(self.nb_of_cc_with_no_coding_per_table)

        # 2. compute the report per table and print it
        self.compute_report_per_table()
        self.print_report()

    def get_counts_instances(self) -> dict:
        counts = {}
        for table_name in TableNames.data_tables():
            counts[table_name] = self.database.count_documents(table_name=table_name, filter_dict={})
        return counts

    def get_nb_of_rec_with_no_value(self) -> dict:
        # for each RecordX, count the number of instances with no field "value"
        counts = {}
        for table_name in TableNames.records():
            count = self.database.count_documents(table_name=table_name, filter_dict={"value": {"$exists": 0}})
            counts[table_name] = count
        return counts

    def get_nb_of_rec_with_no_value_per_instantiate(self) -> dict:
        # for each RecordX, get the distinct list of "instantiate" references that do not have a value in the Record
        # db["LaboratoryRecord"].distinct("instantiate", {"value": {"$exists": 0}})
        # this query returns something like [ { reference: '83' }, { reference: '87' } ]
        # then we process it to return a dict <ref. id, count>, e.g. { "83": 5, "87": 1 }
        # TODO NELLY: count instances for each instantiate
        all_counts = {}
        for table_name in TableNames.records():
            cursor = self.database.find_distinct_operation(table_name=table_name, key="instantiate", filter={"value": {"$exists": 0}})
            counts = {}
            for res in cursor:
                counts[res["reference"]] = 0  # TODO NELLY: set the count to the corresponding nb of instances
            all_counts[table_name] = counts
        return all_counts

    def get_nb_of_cc_with_no_text_per_table(self) -> dict:
        # db["LaboratoryFeature"].find({ "code.text": "" })
        counts = {}
        for table_name in TableNames.features_and_records():
            counts[table_name] = self.database.count_documents(table_name=table_name, filter_dict={"code.text": ""})
        return counts

    def get_nb_of_cc_with_no_coding_per_table(self) -> dict:
        # db["LaboratoryFeature"].find({ "code.coding": [] })
        counts = {}
        for table_name in TableNames.features_and_records():
            counts[table_name] = self.database.count_documents(table_name=table_name, filter_dict={"code.coding": []})
        return counts

    def compute_report_per_table(self) -> None:
        self.report_per_table = {}

        # all RecordX and FeatureX tables
        for table_name in TableNames.features_and_records():
            log.info(table_name)
            self.report_per_table[table_name] = {}
            if table_name in self.counts_instances:
                self.report_per_table[table_name][Reporting.NB_INSTANCES] = self.counts_instances[table_name]
            # else:
            #     self.report_per_table[table_name][Reporting.NB_INSTANCES] = None

            if table_name in self.nb_of_rec_with_no_value:
                self.report_per_table[table_name][Reporting.REC_NO_VALUE] = self.nb_of_rec_with_no_value[table_name]
            # else:
            #     self.report_per_table[table_name][Reporting.REC_NO_VALUE] = None

            if table_name in self.nb_of_rec_with_no_value_per_instantiate:
                self.report_per_table[table_name][Reporting.REC_NO_VALUE_INST] = self.nb_of_rec_with_no_value_per_instantiate[table_name]
            # else:
            #     self.report_per_table[table_name][Reporting.REC_NO_VALUE_INST] = None

            if table_name in  self.nb_of_cc_with_no_text_per_table:
                self.report_per_table[table_name][Reporting.CC_NO_TEXT] = self.nb_of_cc_with_no_text_per_table[table_name]
            # else:
            #     self.report_per_table[table_name][Reporting.CC_NO_TEXT] = None

            if table_name in self.nb_of_cc_with_no_coding_per_table:
                self.report_per_table[table_name][Reporting.CC_NO_CODING] = self.nb_of_cc_with_no_coding_per_table[table_name]
            # else:
            #     self.report_per_table[table_name][Reporting.CC_NO_CODING] = None

        # Patient table
        self.report_per_table[TableNames.PATIENT] = {}
        if TableNames.PATIENT in self.counts_instances:
            self.report_per_table[TableNames.PATIENT][Reporting.NB_INSTANCES] = self.counts_instances[TableNames.PATIENT]
        # else:
        #     self.report_per_table[TableNames.PATIENT][Reporting.NB_INSTANCES] = None

        # Hospital table
        self.report_per_table[TableNames.HOSPITAL] = {}
        if TableNames.HOSPITAL in self.counts_instances:
            self.report_per_table[TableNames.HOSPITAL][Reporting.NB_INSTANCES] = self.counts_instances[TableNames.HOSPITAL]
        # else:
        #     self.report_per_table[TableNames.HOSPITAL][Reporting.NB_INSTANCES] = None

    def print_report(self) -> None:
        log.info("**** FINAL REPORT ****")
        log.info(self.report_per_table)
