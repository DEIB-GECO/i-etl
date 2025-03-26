import dataclasses

from database.Database import Database
from entities.Record import Record
from entities.Resource import Resource
from enums.TableNames import TableNames
from statistics.Statistics import Statistics


@dataclasses.dataclass(kw_only=True)
class DatabaseStatistics(Statistics):
    counts_instances: dict = dataclasses.field(default_factory=dict)
    records_with_no_value: dict = dataclasses.field(default_factory=dict)
    records_with_no_value_per_instantiate: dict = dataclasses.field(default_factory=dict)
    cc_with_no_text_per_table: dict = dataclasses.field(default_factory=dict)
    cc_with_no_onto_resource_per_table: dict = dataclasses.field(default_factory=dict)
    unknown_patient_refs_per_table: dict = dataclasses.field(default_factory=dict)
    unknown_hospital_refs_per_table: dict = dataclasses.field(default_factory=dict)
    unknown_feat_refs_in_records: dict = dataclasses.field(default_factory=dict)

    def compute_stats(self, database: Database):
        if self.record_stats:
            self.compute_counts_instances(database=database)
            self.compute_rec_with_no_value(database=database)
            self.compute_rec_with_no_value_per_instantiate(database=database)
            self.compute_onto_resources_with_no_label_per_table(database=database)
            self.compute_unknown_patient_refs_per_record_table(database=database)
            self.compute_unknown_hospital_refs_per_record_table(database=database)
            self.compute_unknown_feat_refs_in_records(database=database)

    @classmethod
    def jsonify_tuple(cls, one_tuple: dict) -> dict:
        return {key: str(value) for key, value in one_tuple.items()}

    def compute_counts_instances(self, database: Database) -> None:
        for table_name in [TableNames.HOSPITAL, TableNames.PATIENT, TableNames.FEATURE, TableNames.RECORD]:
            if table_name not in self.counts_instances:
                self.counts_instances[table_name] = {}
            self.counts_instances[table_name] = database.count_documents(table_name=table_name, filter_dict={})

    def compute_rec_with_no_value(self, database: Database) -> None:
        # for each RecordX, count the number of instances with no field "value"
        no_val_records = [DatabaseStatistics.jsonify_tuple(res) for res in database.find_operation(table_name=TableNames.RECORD, filter_dict={Record.VALUE_: {"$exists": 0}}, projection={"_id": 0})]
        self.records_with_no_value[TableNames.RECORD] = {"elements": no_val_records, "size": len(no_val_records)}

    def compute_rec_with_no_value_per_instantiate(self, database: Database) -> None:
        # for each RecordX, get the distinct list of "instantiates" references that do not have a value in the Record
        # db["LaboratoryRecord"].distinct("instantiates", {"value": {"$exists": 0}})
        # this query returns something like [ { reference: '83' }, { reference: '87' } ]
        # then we process it to return a dict <ref. id, count>, e.g. { "83": {"elements": [...], "size": 5}, "87": {...} }
        instantiates_no_value = [res for res in database.find_distinct_operation(table_name=TableNames.RECORD, key=Record.INSTANTIATES_, filter_dict={Record.VALUE_: {"$exists": 0}})]
        records_with_no_val_per_instantiate = {}
        for instantiate_ref in instantiates_no_value:
            records_with_no_val_per_instantiate[instantiate_ref] = [DatabaseStatistics.jsonify_tuple(res) for res in database.find_operation(table_name=TableNames.RECORD, filter_dict={Record.INSTANTIATES_: instantiate_ref, Record.VALUE_: {"$exists": 0}}, projection={"_id": 0})]
            if TableNames.RECORD not in self.records_with_no_value_per_instantiate:
                self.records_with_no_value_per_instantiate[TableNames.RECORD] = {}
            self.records_with_no_value_per_instantiate[TableNames.RECORD][instantiate_ref] = {"elements": records_with_no_val_per_instantiate, "size": len(records_with_no_val_per_instantiate)}

    def compute_onto_resources_with_no_label_per_table(self, database: Database) -> None:
        # db["LaboratoryFeature"].find({ "ontology_resource.label": "" })
        for table_name in [TableNames.FEATURE, TableNames.RECORD]:
            if table_name not in self.cc_with_no_text_per_table:
                self.cc_with_no_text_per_table[table_name] = {}
            no_text_cc = [DatabaseStatistics.jsonify_tuple(res) for res in database.find_operation(table_name=table_name, filter_dict={"ontology_resource.label": ""}, projection={"_id": 0})]
            self.cc_with_no_text_per_table[table_name] = {"elements": no_text_cc, "size": len(no_text_cc)}

    def compute_unknown_patient_refs_per_record_table(self, database: Database) -> None:
        unknown_patient_refs = [DatabaseStatistics.jsonify_tuple(res) for res in database.inverse_inner_join(name_table_1=TableNames.RECORD, name_table_2=TableNames.PATIENT, foreign_field=Resource.IDENTIFIER_, local_field=Record.SUBJECT_, lookup_name="KnownRefs")]
        self.unknown_patient_refs_per_table[TableNames.RECORD] = {"elements": unknown_patient_refs, "size": len(unknown_patient_refs)}

    def compute_unknown_hospital_refs_per_record_table(self, database: Database) -> None:
        unknown_hospital_refs = [DatabaseStatistics.jsonify_tuple(res) for res in database.inverse_inner_join(name_table_1=TableNames.RECORD, name_table_2=TableNames.HOSPITAL, foreign_field=Resource.IDENTIFIER_, local_field=Record.REG_BY_, lookup_name="KnownRefs")]
        self.unknown_hospital_refs_per_table[TableNames.RECORD] = {"elements": unknown_hospital_refs, "size": len(unknown_hospital_refs)}

    def compute_unknown_feat_refs_in_records(self, database: Database) -> None:
        unknown_refs = [DatabaseStatistics.jsonify_tuple(res) for res in database.inverse_inner_join(name_table_1=TableNames.RECORD, name_table_2=TableNames.FEATURE, foreign_field=Resource.IDENTIFIER_, local_field=Record.INSTANTIATES_, lookup_name="KnownRefs")]
        self.unknown_feat_refs_in_records = {"elements": unknown_refs, "size": len(unknown_refs)}
