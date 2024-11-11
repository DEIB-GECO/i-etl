from database.Database import Database
from enums.TableNames import TableNames
from statistics.Statistics import Statistics
from utils.mongodb_utils import jsonify_tuple


class DatabaseStatistics(Statistics):
    def __init__(self, record_stats: bool):
        super().__init__(record_stats)

        # counts over the database after the ETL has finished
        self.counts_instances = {}
        self.records_with_no_value = {}
        self.records_with_no_value_per_instantiate = {}
        self.cc_with_no_text_per_table = {}
        self.cc_with_no_onto_resource_per_table = {}
        self.unknown_patient_refs_per_table = {}
        self.unknown_hospital_refs_per_table = {}
        self.unknown_lab_feat_refs_in_lab_record = {}
        self.unknown_diag_feat_refs_in_diag_record = {}
        self.unknown_med_feat_refs_in_med_record = {}
        self.unknown_gen_feat_refs_in_gen_record = {}
        self.unknown_img_feat_refs_in_img_record = {}

    def compute_stats(self, database: Database):
        if self.record_stats:
            self.compute_counts_instances(database=database)
            self.compute_rec_with_no_value(database=database)
            self.compute_rec_with_no_value_per_instantiate(database=database)
            self.compute_onto_resources_with_no_label_per_table(database=database)
            self.compute_unknown_patient_refs_per_record_table(database=database)
            self.compute_unknown_hospital_refs_per_record_table(database=database)
            self.compute_unknown_lab_feat_refs_in_lab_feature(database=database)
            self.compute_unknown_diag_feat_refs_in_diag_feature(database=database)
            self.compute_unknown_med_feat_refs_in_med_feature(database=database)
            self.compute_unknown_gen_feat_refs_in_gen_feature(database=database)
            self.compute_unknown_img_feat_refs_in_img_feature(database=database)

    def compute_counts_instances(self, database: Database) -> None:
        for table_name in TableNames.data_tables(db=database):
            if table_name not in self.counts_instances:
                self.counts_instances[table_name] = {}
            self.counts_instances[table_name] = database.count_documents(table_name=table_name, filter_dict={})

    def compute_rec_with_no_value(self, database: Database) -> None:
        # for each RecordX, count the number of instances with no field "value"
        for table_name in TableNames.records(db=database):
            no_val_records = [jsonify_tuple(res) for res in database.find_operation(table_name=table_name, filter_dict={"value": {"$exists": 0}}, projection={})]
            self.records_with_no_value[table_name] = {"elements": no_val_records, "size": len(no_val_records)}

    def compute_rec_with_no_value_per_instantiate(self, database: Database) -> None:
        # for each RecordX, get the distinct list of "instantiate" references that do not have a value in the Record
        # db["LaboratoryRecord"].distinct("instantiate", {"value": {"$exists": 0}})
        # this query returns something like [ { reference: '83' }, { reference: '87' } ]
        # then we process it to return a dict <ref. id, count>, e.g. { "83": {"elements": [...], "size": 5}, "87": {...} }
        for table_name in TableNames.records(db=database):
            instantiates_no_value = [res for res in database.find_distinct_operation(table_name=table_name, key="instantiate", filter_dict={"value": {"$exists": 0}})]
            records_with_no_val_per_instantiate = {}
            for instantiate_ref in instantiates_no_value:
                records_with_no_val_per_instantiate[instantiate_ref] = [jsonify_tuple(res) for res in database.find_operation(table_name=table_name, filter_dict={"instantiate": instantiate_ref, "value": {"$exists": 0}}, projection={})]
                if table_name not in self.records_with_no_value_per_instantiate:
                    self.records_with_no_value_per_instantiate[table_name] = {}
                self.records_with_no_value_per_instantiate[table_name][instantiate_ref] = {"elements": records_with_no_val_per_instantiate, "size": len(records_with_no_val_per_instantiate)}

    def compute_onto_resources_with_no_label_per_table(self, database: Database) -> None:
        # db["LaboratoryFeature"].find({ "ontology_resource.label": "" })
        for table_name in TableNames.features_and_records(db=database):
            if table_name not in self.cc_with_no_text_per_table:
                self.cc_with_no_text_per_table[table_name] = {}
            no_text_cc = [jsonify_tuple(res) for res in database.find_operation(table_name=table_name, filter_dict={"ontology_resource.label": ""}, projection={})]
            self.cc_with_no_text_per_table[table_name] = {"elements": no_text_cc, "size": len(no_text_cc)}

    def compute_unknown_patient_refs_per_record_table(self, database: Database) -> None:
        for table_name in TableNames.records(db=database):
            unknown_patient_refs = [jsonify_tuple(res) for res in database.inverse_inner_join(name_table_1=table_name, name_table_2=TableNames.PATIENT, field_table_1="identifier", field_table_2="subject", lookup_name="KnownRefs")]
            self.unknown_patient_refs_per_table[table_name] = {"elements": unknown_patient_refs, "size": len(unknown_patient_refs)}

    def compute_unknown_hospital_refs_per_record_table(self, database: Database) -> None:
        for table_name in TableNames.records(db=database):
            unknown_hospital_refs = [jsonify_tuple(res) for res in database.inverse_inner_join(name_table_1=table_name, name_table_2=TableNames.HOSPITAL, field_table_1="identifier", field_table_2="recorded_by", lookup_name="KnownRefs")]
            self.unknown_hospital_refs_per_table[table_name] = {"elements": unknown_hospital_refs, "size": len(unknown_hospital_refs)}

    def compute_unknown_lab_feat_refs_in_lab_feature(self, database: Database) -> None:
        self.unknown_lab_feat_refs_in_lab_record = self.compute_unknown_ref_in_rec(database=database, record_table_name=TableNames.PHENOTYPIC_RECORD)

    def compute_unknown_diag_feat_refs_in_diag_feature(self, database: Database) -> None:
        self.unknown_diag_feat_refs_in_diag_record = self.compute_unknown_ref_in_rec(database=database, record_table_name=TableNames.DIAGNOSIS_RECORD)

    def compute_unknown_med_feat_refs_in_med_feature(self, database: Database) -> None:
        self.unknown_med_feat_refs_in_med_record = self.compute_unknown_ref_in_rec(database=database, record_table_name=TableNames.MEDICINE_RECORD)

    def compute_unknown_gen_feat_refs_in_gen_feature(self, database: Database) -> None:
        self.unknown_gen_feat_refs_in_gen_record = self.compute_unknown_ref_in_rec(database=database, record_table_name=TableNames.GENOMIC_RECORD)

    def compute_unknown_img_feat_refs_in_img_feature(self, database: Database) -> None:
        self.unknown_img_feat_refs_in_img_record = self.compute_unknown_ref_in_rec(database=database, record_table_name=TableNames.IMAGING_RECORD)

    def compute_unknown_ref_in_rec(self, database: Database, record_table_name: str) -> dict:
        feature_table_name = TableNames.get_feature_table_from_record_table(record_table_name=record_table_name)
        unknown_refs = [jsonify_tuple(res) for res in database.inverse_inner_join(name_table_1=record_table_name, name_table_2=feature_table_name, field_table_1="identifier", field_table_2="instantiate", lookup_name="KnownRefs")]
        unknown_refs_for_rec = {"elements": unknown_refs, "size": len(unknown_refs)}
        return unknown_refs_for_rec
