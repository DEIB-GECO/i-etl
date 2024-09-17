import jsonpickle

from database.Database import Database
from enums.TableNames import TableNames
from statistics.Statistics import Statistics
from utils.mongodb_utils import jsonify_tuple
from utils.setup_logger import log


class DatabaseStatistics(Statistics):
    def __init__(self, database: Database, record_stats: bool):
        super().__init__(record_stats)
        self.database = database

        # counts over the database after the ETL has finished
        self.counts_instances = {}
        self.nb_of_rec_with_no_value = {}
        self.nb_of_rec_with_no_value_per_instantiate = {}
        self.nb_of_cc_with_no_text_per_table = {}
        self.nb_of_cc_with_no_coding_per_table = {}

    def compute_stats(self):
        if self.record_stats:
            self.compute_counts_instances()
            log.info(self.counts_instances)
            self.compute_nb_of_rec_with_no_value()
            log.info(self.nb_of_rec_with_no_value)
            self.compute_nb_of_rec_with_no_value_per_instantiate()
            log.info(self.nb_of_rec_with_no_value_per_instantiate)
            self.compute_nb_of_cc_with_no_text_per_table()
            log.info(self.nb_of_cc_with_no_text_per_table)
            self.compute_nb_of_cc_with_no_coding_per_table()
            log.info(self.nb_of_cc_with_no_coding_per_table)

    def compute_counts_instances(self) -> None:
        self.counts_instances = {}
        for table_name in TableNames.data_tables():
            if table_name not in self.counts_instances:
                self.counts_instances[table_name] = {}
            self.counts_instances[table_name] = self.database.count_documents(table_name=table_name, filter_dict={})

    def compute_nb_of_rec_with_no_value(self) -> None:
        # for each RecordX, count the number of instances with no field "value"
        self.nb_of_rec_with_no_value = {}
        for table_name in TableNames.records():
            if table_name not in self.nb_of_rec_with_no_value:
                self.nb_of_rec_with_no_value[table_name] = {}
            no_val_records = [jsonify_tuple(res) for res in self.database.find_operation(table_name=table_name, filter_dict={"value": {"$exists": 0}}, projection={})]
            self.nb_of_rec_with_no_value[table_name] = {"elements": no_val_records, "size": len(no_val_records)}

    def compute_nb_of_rec_with_no_value_per_instantiate(self) -> None:
        # for each RecordX, get the distinct list of "instantiate" references that do not have a value in the Record
        # db["LaboratoryRecord"].distinct("instantiate", {"value": {"$exists": 0}})
        # this query returns something like [ { reference: '83' }, { reference: '87' } ]
        # then we process it to return a dict <ref. id, count>, e.g. { "83": {"elements": [...], "size": 5}, "87": {...} }
        self.nb_of_rec_with_no_value_per_instantiate = {}
        for table_name in TableNames.records():
            log.info(table_name)
            instantiates_no_value = [jsonify_tuple(res) for res in self.database.find_distinct_operation(table_name=table_name, key="instantiate", filter={"value": {"$exists": 0}})]
            log.info(instantiates_no_value)
            records_with_no_val_per_instantiate = {}
            for instantiate in instantiates_no_value:
                instantiate_ref = instantiate["reference"]
                log.info(instantiate)
                log.info(instantiate_ref)
                records_with_no_val_per_instantiate[instantiate_ref] = [jsonify_tuple(res) for res in self.database.find_operation(table_name=table_name, filter_dict={"instantiate": instantiate_ref, "value": {"$exists": 0}}, projection={})]
                log.info(records_with_no_val_per_instantiate)
                if table_name not in self.nb_of_rec_with_no_value_per_instantiate:
                    self.nb_of_rec_with_no_value_per_instantiate[table_name] = {}
                self.nb_of_rec_with_no_value_per_instantiate[table_name][instantiate_ref] = {"elements": records_with_no_val_per_instantiate, "size": len(records_with_no_val_per_instantiate)}

    def compute_nb_of_cc_with_no_text_per_table(self) -> None:
        # db["LaboratoryFeature"].find({ "code.text": "" })
        self.nb_of_cc_with_no_text_per_table = {}
        for table_name in TableNames.features_and_records():
            if table_name not in self.nb_of_cc_with_no_text_per_table:
                self.nb_of_cc_with_no_text_per_table[table_name] = {}
            no_text_cc = [jsonify_tuple(res) for res in self.database.find_operation(table_name=table_name, filter_dict={"code.text": ""}, projection={})]
            log.info(no_text_cc)
            self.nb_of_cc_with_no_text_per_table[table_name] = {"elements": no_text_cc, "size": len(no_text_cc)}

    def compute_nb_of_cc_with_no_coding_per_table(self) -> None:
        # db["LaboratoryFeature"].find({ "code.coding": [] })
        self.nb_of_cc_with_no_coding_per_table = {}
        for table_name in TableNames.features_and_records():
            if table_name not in self.nb_of_cc_with_no_coding_per_table:
                self.nb_of_cc_with_no_coding_per_table[table_name] = {}
            no_coding_cc = [jsonify_tuple(res) for res in self.database.find_operation(table_name=table_name, filter_dict={"code.coding": []}, projection={})]
            log.info(no_coding_cc)
            self.nb_of_cc_with_no_coding_per_table[table_name] = {"elements": no_coding_cc, "size": len(no_coding_cc)}
