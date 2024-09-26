import json
import re
from datetime import datetime
from typing import Any

from pandas import DataFrame

from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.Identifier import Identifier
from datatypes.OntologyResource import OntologyResource
from enums.DataTypes import DataTypes
from enums.FileTypes import FileTypes
from enums.LabFeatureCategories import LabFeatureCategories
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.SampleColumns import SampleColumns
from enums.TableNames import TableNames
from enums.TrueFalse import TrueFalse
from enums.Visibility import Visibility
from etl.Task import Task
from profiles.DiagnosisFeature import DiagnosisFeature
from profiles.DiagnosisRecord import DiagnosisRecord
from profiles.GenomicFeature import GenomicFeature
from profiles.GenomicRecord import GenomicRecord
from profiles.Hospital import Hospital
from profiles.LaboratoryFeature import LaboratoryFeature
from profiles.LaboratoryRecord import LaboratoryRecord
from profiles.Patient import Patient
from profiles.SampleRecord import SampleRecord
from profiles.SampleFeature import SampleFeature
from database.Counter import Counter
from constants.defaults import BATCH_SIZE, PATTERN_VALUE_DIMENSION
from constants.idColumns import NO_ID, ID_COLUMNS
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.assertion_utils import is_not_nan
from utils.cast_utils import cast_str_to_boolean, cast_str_to_datetime, cast_str_to_float, cast_str_to_int
from utils.file_utils import write_in_file
from utils.setup_logger import log


class Transform(Task):

    def __init__(self, database: Database, execution: Execution, data: DataFrame, metadata: DataFrame,
                 mapping_categorical_value_to_cc: dict, mapping_column_to_categorical_value: dict,
                 mapping_column_to_dimension: dict, patient_ids_mapping: dict,
                 mapping_diagnosis_to_cc: dict, mapping_sample_id_to_patient_id: dict,
                 quality_stats: QualityStatistics, time_stats: TimeStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats, time_stats=time_stats)
        self.counter = Counter()

        # get data, metadata and the mapped values computed in the Extract step
        self.data = data
        self.metadata = metadata
        self.mapping_column_to_dimension = mapping_column_to_dimension
        self.mapping_categorical_value_to_cc = mapping_categorical_value_to_cc
        self.mapping_column_to_categorical_value = mapping_column_to_categorical_value
        # to keep track of anonymized vs. hospital patient ids
        # this is empty if no file as been provided by the user, otherwise it contains some mappings <patient ID, anonymized ID>
        self.patient_ids_mapping = patient_ids_mapping
        self.mapping_diagnosis_to_cc = mapping_diagnosis_to_cc  # it may be None if this is not BUZZI
        self.mapping_sample_id_to_patient_id = mapping_sample_id_to_patient_id  # it may be None if this is not BUZZI

        # to record objects that will be further inserted in the database
        self.features = []
        self.records = []
        self.hospitals = []
        self.patients = []
        self.samples = []

    def run(self) -> None:
        self.set_resource_counter_id()
        self.create_hospital()
        log.info(f"Hospital count: {self.database.count_documents(table_name=TableNames.HOSPITAL, filter_dict={})}")

        if self.execution.current_file_type == FileTypes.LABORATORY:
            log.info("********** create lab features")
            self.create_laboratory_features()
            log.info("********** create lab records")
            self.create_laboratory_records()
            log.info("********** done with lab data")
        elif self.execution.current_file_type == FileTypes.SAMPLE:
            log.info("********** create patients")
            self.create_patients()
            log.info("********** create sample features")
            self.create_sample_features()
            log.info("********** create sample records")
            self.create_sample_records()
        elif self.execution.current_file_type == FileTypes.DIAGNOSIS:
            self.create_diagnosis_features()
            self.create_diagnosis_records()
        elif self.execution.current_file_type == FileTypes.MEDICINE:
            self.create_medicine_features()
            self.create_medicine_records()
        elif self.execution.current_file_type == FileTypes.IMAGING:
            self.create_imaging_features()
            self.create_imaging_records()
        elif self.execution.current_file_type == FileTypes.GENOMIC:
            log.info("process genomic data!")
            log.info(self.metadata)
            log.info(self.data)
            self.create_genomic_features()
            self.create_genomic_records()
        else:
            raise TypeError(f"The current file type ({self.execution.current_file_type} is unknown. It should be laboratory, diagnosis, medicine, imaging or genomic.")

    ##############################################################
    # FEATURES
    ##############################################################

    def create_features(self, table_name: str) -> None:
        # 1. get existing features in memory
        result = self.database.find_operation(table_name=table_name, filter_dict={}, projection={"code.text": 1})
        db_existing_features = [res["code"]["text"] for res in result]

        # 2. create non-existing features in-memory, then insert them
        log.info(f"create Feature instances of type {table_name} in memory")
        count = 1
        for row_index, row in self.metadata.iterrows():
            column_name = row[MetadataColumns.COLUMN_NAME]
            # columns to remove have already been removed in the Extract part from the metadata
            # here, we need to ensure that we create Features only for still-existing columns and not for ID column
            if (column_name in self.metadata[MetadataColumns.COLUMN_NAME].values
                    and column_name != ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                    and (TableNames.SAMPLE_RECORD not in ID_COLUMNS[self.execution.hospital_name]
                         or (TableNames.SAMPLE_RECORD in ID_COLUMNS[self.execution.hospital_name] and column_name != ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE_RECORD])
                    )):
                if column_name not in db_existing_features:
                    # we create a new Feature from scratch
                    cc = self.create_codeable_concept_from_row(column_name=column_name)
                    data_type = row[MetadataColumns.ETL_TYPE]  # this has been normalized while loading + we take ETL_type, not var_type, to get the narrowest type (in which we cast values)
                    visibility = row[MetadataColumns.VISIBILITY]  # this has been normalized while loading
                    dimension = self.mapping_column_to_dimension[column_name] if column_name in self.mapping_column_to_dimension else None  # else covers: there is no dataType for this column; there is no datatype in that type of entity
                    categorical_values = None
                    if column_name in self.mapping_column_to_categorical_value:
                        # for categorical values, we first need to take the list of (normalized) values that are available for the current column, and then take their CC
                        normalized_categorical_values = self.mapping_column_to_categorical_value[column_name]
                        categorical_values = [self.mapping_categorical_value_to_cc[normalized_categorical_value] for normalized_categorical_value in normalized_categorical_values]
                    if cc is not None:
                        # some columns have no attributed ontology code
                        # we still add the codeable_concept as a LabFeature
                        # but, it will have only the text (such as "BIS", or "TooYoung") and no associated codings
                        if table_name == TableNames.LABORATORY_FEATURE:
                            if column_name == SampleColumns.SAMPLE_BAR_CODE:
                                # we kept this column to be able to associate LabRecords with SampleRecords
                                # but this column will be created as a Feature in the SampleFeature table
                                new_feature = None
                            else:
                                category = self.get_lab_feature_category(column_name=column_name)
                                new_feature = LaboratoryFeature(id_value=NO_ID, code=cc, category=category,
                                                                permitted_datatype=data_type, dimension=dimension,
                                                                counter=self.counter, hospital_name=self.execution.hospital_name,
                                                                categorical_values=categorical_values, visibility=visibility)
                        elif table_name == TableNames.SAMPLE_FEATURE:
                            # we limit the creation of sample features to a subset of laboratory columns
                            # but there is a single file that contains both clinical and sample data
                            new_feature = SampleFeature(id_value=NO_ID, code=cc,
                                                        permitted_datatype=data_type, dimension=dimension,
                                                        counter=self.counter,
                                                        hospital_name=self.execution.hospital_name,
                                                        categorical_values=categorical_values,
                                                        visibility=visibility)
                        elif table_name == TableNames.DIAGNOSIS_FEATURE:
                            new_feature = DiagnosisFeature(id_value=NO_ID, code=cc,
                                                            permitted_datatype=data_type,
                                                            dimension=dimension, counter=self.counter,
                                                            hospital_name=self.execution.hospital_name,
                                                            categorical_values=categorical_values, visibility=visibility)
                        elif table_name == TableNames.GENOMIC_FEATURE:
                            new_feature = GenomicFeature(id_value=NO_ID, code=cc,
                                                            permitted_datatype=data_type,
                                                            dimension=dimension, counter=self.counter,
                                                            hospital_name=self.execution.hospital_name,
                                                            categorical_values=categorical_values, visibility=visibility)
                        else:
                            raise NotImplementedError("To be implemented")

                        if new_feature is not None:
                            log.info(f"adding a new {table_name} instance about {cc.text}: {new_feature}")
                            self.features.append(new_feature)
                        if len(self.features) >= BATCH_SIZE:
                            write_in_file(resource_list=self.features,
                                          current_working_dir=self.execution.working_dir_current,
                                          table_name=table_name, count=count)
                            self.features.clear()
                            count = count + 1
                    else:
                        # the column was not present in the data, or it was present several times
                        # this should never happen
                        pass
                else:
                    # the LabFeature already exists, so no need to add it to the database again.
                    log.error(f"The lab feature about {column_name} already exists. Not added.")
            else:
                log.debug(f"I am skipping column {column_name} because it has been marked as not being part of LabFeature instances.")
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        self.write_remaining_instances_to_database(resources=self.features, table_name=table_name, count=count, unique_variables=["code"])

    def create_laboratory_features(self) -> None:
        self.create_features(table_name=TableNames.LABORATORY_FEATURE)

    def create_sample_features(self) -> None:
        self.create_features(table_name=TableNames.SAMPLE_FEATURE)

    def create_diagnosis_features(self):
        self.create_features(table_name=TableNames.DIAGNOSIS_FEATURE)

    def create_medicine_features(self):
        # TODO Nelly: implement this
        pass

    def create_imaging_features(self):
        # TODO Nelly: implement this
        pass

    def create_genomic_features(self):
        self.create_features(table_name=TableNames.GENOMIC_FEATURE)

    ##############################################################
    # RECORDS
    ##############################################################

    def create_records(self, table_name: str) -> None:
        log.info(f"create {table_name} instances in memory")

        # a. load some data from the database to compute references
        mapping_hospital_to_hospital_id = self.database.retrieve_mapping(table_name=TableNames.HOSPITAL, key_fields="name", value_fields="identifier")
        log.debug(f"{mapping_hospital_to_hospital_id}")

        feature_table_name = TableNames.get_feature_table_from_record_table(record_table_name=table_name)
        mapping_column_to_feature_id = self.database.retrieve_mapping(table_name=feature_table_name, key_fields="code.text", value_fields="identifier")
        log.debug(f"{mapping_column_to_feature_id}")

        mapping_sample_id_to_sample_record_id = self.database.retrieve_mapping(table_name=TableNames.SAMPLE_RECORD, key_fields="sample_base_id", value_fields="identifier")
        log.debug(f"{mapping_sample_id_to_sample_record_id}")

        # b. Create Record instance, and write them in temporary (JSON) files
        count = 1
        for index, row in self.data.iterrows():
            # create Record instances by associating observations to patients
            for column_name, value in row.items():
                # log.debug(f"for row {index} (type: {type(index)}) and column {column_name} (type: {type(column_name)}), value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that Feature, no need to create a Record instance
                    # log.error(f"skipping value {value} because it is None, or empty or nan")
                    pass
                else:
                    if column_name in mapping_column_to_feature_id:
                        # we know a code for this column, so we can register the value of that Feature in a new Record
                        feature_id = Identifier(value=mapping_column_to_feature_id[column_name])
                        hospital_id = Identifier(mapping_hospital_to_hospital_id[self.execution.hospital_name])
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                        # we cannot use PatientAnonymizedIdentifier otherwise we would concat a set hospital name to the anonymized patient id
                        # patient_id = PatientAnonymizedIdentifier(id_value=self.patient_ids_mapping[row[id_column_for_patients]], hospital_name=self.execution.hospital_name)
                        patient_id = Identifier(value=self.patient_ids_mapping[row[id_column_for_patients]])
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        anonymized_value, is_anonymized = self.anonymize_value(column_name=column_name, fairified_value=fairified_value)
                        if table_name == TableNames.LABORATORY_RECORD:
                            if column_name == SampleColumns.SAMPLE_BAR_CODE:
                                # we kept this column to be able to associate LabRecords to SampleRecords
                                # but, we don't want to register this as a LabRecord.
                                new_record = None
                            else:
                                if SampleColumns.SAMPLE_BAR_CODE in row:
                                    sample_base_id = row[SampleColumns.SAMPLE_BAR_CODE]
                                    sample_record_id = Identifier(value=mapping_sample_id_to_sample_record_id[
                                        sample_base_id]) if sample_base_id in mapping_sample_id_to_sample_record_id else None
                                else:
                                    sample_record_id = None
                                new_record = LaboratoryRecord(id_value=NO_ID, feature_id=feature_id,
                                                                 patient_id=patient_id, hospital_id=hospital_id,
                                                                 sample_id=sample_record_id,
                                                                 value=fairified_value,
                                                                 anonymized_value=anonymized_value if is_anonymized else None,
                                                                 counter=self.counter,
                                                                 hospital_name=self.execution.hospital_name)
                        elif table_name == TableNames.SAMPLE_RECORD:
                            # we should definitely create a record for this (column, value) because this is about a sample column
                            new_record = SampleRecord(id_value=NO_ID, feature_id=feature_id, patient_id=patient_id,
                                                      hospital_id=hospital_id, value=fairified_value,
                                                      anonymized_value=anonymized_value if is_anonymized else None,
                                                      sample_base_id=row[SampleColumns.SAMPLE_BAR_CODE],
                                                      counter=self.counter, hospital_name=self.execution.hospital_name)
                        elif table_name == TableNames.DIAGNOSIS_RECORD:
                            if TableNames.SAMPLE_RECORD in ID_COLUMNS[self.execution.hospital_name]:
                                # https://github.com/Nelly-Barret/BETTER-fairificator/issues/146
                                # in this case, the ID in the diagnosis data is the SAMPLE id, not the patient one
                                # thus, we need to get it by hand (to effectively record a patient ID)
                                # in any case, clinical data MUST be ingested before the diagnosis data, otherwise the mapping wil be empty
                                sample_id = row[ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE_RECORD]]
                                if sample_id in self.mapping_sample_id_to_patient_id:
                                    patient_id = Identifier(value=self.mapping_sample_id_to_patient_id[sample_id])
                                else:
                                    # in case this sample has no associated sample barcode
                                    # we take the sample bar code (which is, at least, a bit better than None)
                                    patient_id = Identifier(value=sample_id)
                            else:
                                # this is the normal case, we can get the patient ID directly from the data
                                id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                                patient_id = Identifier(value=self.patient_ids_mapping[row[id_column_for_patients]])
                            if fairified_value is None:
                                # the patient has genetic mutation, but he is not ill
                                # thus, we do not record it in database
                                new_record = None
                            else:
                                new_record = DiagnosisRecord(id_value=NO_ID, feature_id=feature_id,
                                                                  patient_id=patient_id, hospital_id=hospital_id,
                                                                  value=fairified_value,
                                                                  anonymized_value=anonymized_value if is_anonymized else None,
                                                                  counter=self.counter,
                                                                  hospital_name=self.execution.hospital_name)
                        elif table_name == TableNames.GENOMIC_RECORD:
                            new_record = GenomicRecord(id_value=NO_ID, feature_id=feature_id,
                                                          patient_id=patient_id, hospital_id=hospital_id,
                                                          uri=None,
                                                          value=fairified_value,
                                                          anonymized_value=anonymized_value if is_anonymized else None,
                                                          counter=self.counter,
                                                          hospital_name=self.execution.hospital_name)
                        else:
                            raise NotImplementedError("Not implemented yet.")
                        if new_record is not None:  # it can be None if the patient is a carrier but not diseased
                            self.records.append(new_record)
                        if len(self.records) >= BATCH_SIZE:
                            log.info(f"writing {len(self.records)}")
                            write_in_file(resource_list=self.records, current_working_dir=self.execution.working_dir_current,
                                          table_name=table_name, count=count)
                            self.records.clear()
                            count = count + 1
                    else:
                        # this represents the case when a column has not been converted to a LabFeature resource
                        # this may happen for ID column for instance, or in BUZZI many clinical columns are not described in the metadata, thus skipped here
                        # log.error(f"Skipping column {column_name} for row {index}")
                        pass
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        self.write_remaining_instances_to_database(resources=self.records, table_name=table_name, count=count, unique_variables=["code"])

    def create_laboratory_records(self) -> None:
        self.create_records(table_name=TableNames.LABORATORY_RECORD)

    def create_sample_records(self) -> None:
        self.create_records(table_name=TableNames.SAMPLE_RECORD)

    def create_diagnosis_records(self):
        self.create_records(table_name=TableNames.DIAGNOSIS_RECORD)

    def create_medicine_records(self):
        # TODO Nelly: implement this
        pass

    def create_imaging_records(self):
        # TODO Nelly: implement this
        pass

    def create_genomic_records(self):
        self.create_records(table_name=TableNames.GENOMIC_RECORD)

    ##############################################################
    # OTHER ENTITIES
    ##############################################################

    def create_patients(self) -> None:
        log.info(f"create patient instances in memory")
        count = 1
        for index, row in self.data.iterrows():
            row_patient_id = row[ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]]
            if row_patient_id not in self.patient_ids_mapping:
                # the (anonymized) patient does not exist yet, we will create it
                new_patient = Patient(id_value=NO_ID, counter=self.counter, hospital_name=self.execution.hospital_name)
                self.patient_ids_mapping[row_patient_id] = new_patient.identifier.value  # keep track of anonymized patient ids
            else:
                # the (anonymized) patient id already exists, we take it from the mapping
                new_patient = Patient(id_value=self.patient_ids_mapping[row_patient_id], counter=self.counter, hospital_name=self.execution.hospital_name)
            self.patients.append(new_patient)
            if len(self.patients) >= BATCH_SIZE:
                write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current, table_name=TableNames.PATIENT, count=count)  # this will save the data if it has reached BATCH_SIZE
                self.patients = []
                count = count + 1
                # no need to load Patient instances because they are referenced using their ID,
                # which was provided by the hospital (thus is known by the dataset)
        write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current, table_name=TableNames.PATIENT, count=count)
        # finally, we also write the mapping patient ID / anonymized ID in a file - this will be ingested for subsequent runs to not renumber existing anonymized patients
        with open(self.execution.anonymized_patient_ids_filepath, "w") as data_file:
            try:
                json.dump(self.patient_ids_mapping, data_file)
            except Exception:
                raise ValueError(f"Could not dump the {len(self.patient_ids_mapping)} JSON resources in the file located at {self.execution.anonymized_patient_ids_filepath}.")

    ##############################################################
    # UTILITIES
    ##############################################################

    def set_resource_counter_id(self) -> None:
        self.counter.set_with_database(database=self.database)

    def create_hospital(self) -> None:
        log.info(f"create hospital instance in memory")
        new_hospital = Hospital(id_value=NO_ID, name=self.execution.hospital_name, counter=self.counter)
        self.hospitals.append(new_hospital)
        write_in_file(resource_list=self.hospitals, current_working_dir=self.execution.working_dir_current, table_name=TableNames.HOSPITAL, count=1)
        self.database.load_json_in_table(table_name=TableNames.HOSPITAL, unique_variables=["name"])

    def write_remaining_instances_to_database(self, resources: list, table_name: str, count: int, unique_variables: list[str]):
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        write_in_file(resource_list=resources, current_working_dir=self.execution.working_dir_current,
                      table_name=table_name, count=count)
        self.database.load_json_in_table(table_name=table_name, unique_variables=unique_variables)

    def create_codeable_concept_from_row(self, column_name: str) -> CodeableConcept | None:
        cc = CodeableConcept(original_name=column_name)
        rows = self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name]
        if len(rows) == 1:
            row = rows.iloc[0]
            if is_not_nan(row[MetadataColumns.ONTO_NAME_1]):
                coding = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(row[MetadataColumns.ONTO_NAME_1]),
                                                      full_code=row[MetadataColumns.ONTO_CODE_1], quality_stats=self.quality_stats),
                                display=None)
                cc.add_coding(one_coding=coding)
            if is_not_nan(row[MetadataColumns.ONTO_NAME_2]):
                coding = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(row[MetadataColumns.ONTO_NAME_2]),
                                                      full_code=row[MetadataColumns.ONTO_CODE_2], quality_stats=self.quality_stats),
                                display=None)
                cc.add_coding(one_coding=coding)
            return cc
        elif len(rows) == 0:
            # log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            # log.warn("Found several times the column '%s' in the metadata", column_name)
            return None

    def is_column_phenotypic(self, column_name: str) -> bool:
        return cast_str_to_boolean(self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name][MetadataColumns.PHENOTYPIC].iloc[0])

    def get_lab_feature_category(self, column_name: str) -> CodeableConcept:
        if self.is_column_phenotypic(column_name=column_name):
            return LabFeatureCategories.get_phenotypic()
        else:
            return LabFeatureCategories.get_clinical()

    def fairify_value(self, column_name: str, value: Any) -> str | float | datetime | CodeableConcept:
        current_column_info = DataFrame(self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name])
        etl_type = current_column_info[MetadataColumns.ETL_TYPE].iloc[0]  # we need to take the value itself, otherwise we end up with a series of 1 element (the ETL type)
        the_normalized_value = MetadataColumns.normalize_value(column_value=value)  # we compute only once the cast value and return it whenever we cannot FAIRify deeply the value
        return_value = None  # to ease logging, we also save the return value in a variable and return it at the very end
        expected_unit = self.mapping_column_to_dimension[column_name] if column_name in self.mapping_column_to_dimension else None  # there was some unit specified in the metadata or extracted from the data

        # log.debug(f"ETL type is {etl_type}, var type is {var_type}, expected unit is {expected_unit}")
        if etl_type == DataTypes.STRING:
            return value  # the value is already normalized, we can return it as is
        elif etl_type == DataTypes.CATEGORY:
            # we look for the CC associated to that categorical value
            # we need to check that (a) the column expects this categorical value and (b) this categorical has an associated CC
            if column_name in self.mapping_column_to_categorical_value and value in self.mapping_column_to_categorical_value[column_name] and value in self.mapping_categorical_value_to_cc:
                return_value = self.mapping_categorical_value_to_cc[value]
            else:
                # no categorical value for that value, we return the normalized value
                self.quality_stats.add_unknown_categorical_value(column_name=column_name, categorical_value=value)
                return_value = the_normalized_value
        elif etl_type == DataTypes.DATETIME or etl_type == DataTypes.DATE:
            return_value = cast_str_to_datetime(str_value=value)
        elif etl_type == DataTypes.BOOLEAN:
            # boolean values may appear as (a) CC (si/no or 0/1), or (b) 0/1 or 0.0/1.0 (1.0/0.0 has to be converted to 1/0)
            value = "1" if value == "1.0" else "0" if value == "0.0" else value
            # and that same value is also available in the mapping to cc
            if column_name in self.mapping_column_to_categorical_value and value in self.mapping_column_to_categorical_value[column_name] and value in self.mapping_categorical_value_to_cc:
                cc = self.mapping_categorical_value_to_cc[value]
                # this should rather be a boolean, let's cast it as boolean, instead of using Yes/No SNOMED_CT codes
                if cc == TrueFalse.get_true():
                    return_value = True
                elif cc == TrueFalse.get_false():
                    return_value = False
                else:
                    self.quality_stats.add_unknown_boolean_value(column_name=column_name, boolean_value=value)
                    return_value = the_normalized_value
            else:
                # no coded value for that value, trying to cast it as boolean
                return_value = cast_str_to_boolean(str_value=value)
        elif etl_type == DataTypes.INTEGER or etl_type == DataTypes.FLOAT:
            if column_name == ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]:
                # do not cast int-like string identifiers as integers because this may add too much normalization
                return_value = str(value)
            else:
                # this is really a numeric value that we want to cast
                # several cases:
                # - no dimension in metadata, no dimension in data: cast
                # - no dimension in metadata, one dimension in data: extract the dimension + cast
                # - no dimension in metadata, several dimensions in data: get most frequent one, data values with the frequent dim are cast, others are left as strings
                # - one dimension in metadata, no dimension in data: cast
                # - one dimension in metadata, one dimensions in data, they are equal: remove dimension from data + cast
                # - one dimension in metadata, one dimensions in data, they are not equal: leave data as string
                # - one dimension in metadata, several dimensions in data: data values with the metadata dim are cast, others are left as strings
                m = re.search(PATTERN_VALUE_DIMENSION, value)
                if m is not None:
                    # there is a dimension in the data value
                    # m.group(0) is the text itself, m.group(1) is the int/float value, m.group(2) is the dimension
                    the_value = m.group(1)
                    unit = m.group(2)
                    if unit == expected_unit:
                        if etl_type == DataTypes.INTEGER:
                            return_value = cast_str_to_int(str_value=the_value)
                        elif etl_type == DataTypes.FLOAT:
                            return_value = cast_str_to_float(str_value=the_value)
                    else:
                        # the feature dimension does not correspond, we return the normalized (string) value
                        self.quality_stats.add_numerical_value_with_unmatched_dimension(column_name=column_name, expected_dimension=expected_unit, current_dimension=unit, value=value)
                        return_value = the_normalized_value
                else:
                    # this value does not contain a dimension or is not of the form "value dimension"
                    # thus, we cast it depending on the ETL type
                    if etl_type == DataTypes.INTEGER:
                        return_value = cast_str_to_int(str_value=value)
                    elif etl_type == DataTypes.FLOAT:
                        return_value = cast_str_to_float(str_value=value)
                    else:
                        return_value = the_normalized_value
        elif etl_type == DataTypes.REGEX:
            # this corresponds to a diagnosis name, for which we need to find the corresponding ORPHANET code
            if value in self.mapping_diagnosis_to_cc:
                # the standard diagnosis name has corresponding codes (ORPHANET and OMIM)
                the_cc = self.mapping_diagnosis_to_cc[value]["cc"]
                return_value = the_cc
            else:
                log.error(f"'{value}' is not described in the companion diagnosis file. Will return '{the_normalized_value}'")
                return_value = the_normalized_value
        else:
            # Unhandled ETL type: this cannot happen because all ETL types have been checked during the Extract step
            return_value = the_normalized_value

        # in case, casting the value returned None, we set the normalized value back
        # otherwise, we keep the cast value
        return_value = the_normalized_value if return_value is None else return_value

        # count how many fairified values do not (still) match the expected ETL type
        if ((etl_type == DataTypes.BOOLEAN and not isinstance(return_value, bool))
                or (etl_type == DataTypes.FLOAT and not isinstance(return_value, float))
                or (etl_type == DataTypes.INTEGER and not isinstance(return_value, int))
                or (etl_type == DataTypes.DATE and not isinstance(return_value, datetime))
                or (etl_type == DataTypes.DATETIME and not isinstance(return_value, datetime))
                or (etl_type == DataTypes.STRING and not isinstance(return_value, str))
                or (etl_type == DataTypes.CATEGORY and not isinstance(return_value, CodeableConcept))):
            self.quality_stats.add_column_with_unmatched_typeof_etl_types(column_name=column_name, typeof_type=type(return_value).__name__, etl_type=etl_type)

        # we use type(..).__name__ to get the class name, e.g., "str" or "bool", instead of "<class 'float'>"
        # log.info(f"Column '{column_name}': fairify {type(value).__name__} value '{value}' (unit: {expected_unit}) into {type(return_value).__name__}: {return_value}")
        return return_value

    def anonymize_value(self, column_name: str, fairified_value: Any) -> tuple:
        """

        :param column_name:
        :type column_name:
        :param fairified_value:
        :type fairified_value:
        :return: two values: either (the anonymized value, True), or (the fairified value, False). This is to know whether we should create another Record with the anonymized value
        :rtype:
        """
        current_column_info = DataFrame(self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name])
        etl_type = current_column_info[MetadataColumns.ETL_TYPE].iloc[0]  # we need to take the value itself, otherwise we end up with a series of 1 element (the ETL type)
        visibility = current_column_info[MetadataColumns.VISIBILITY].iloc[0]
        if etl_type == DataTypes.DATETIME:
            if visibility == Visibility.PUBLIC_WITH_ANONYMIZATION:
                # anonymize the date and the time
                # since a datetime object always contains day+month+year (in any order), we cannot get rid of the day
                # however, we can set it to 01
                # similarly for hour:minute:second, we set it to 0
                anonymized_value = fairified_value.replace(day=1)
                anonymized_value = anonymized_value.replace(hour=0)
                anonymized_value = anonymized_value.replace(minute=0)
                anonymized_value = anonymized_value.replace(second=0)
                return anonymized_value, True
            else:
                # no need to anonymize the datetime
                return fairified_value, False
        elif etl_type == DataTypes.DATE:
            if visibility == Visibility.PUBLIC_WITH_ANONYMIZATION:
                # anonymize the date
                anonymized_value = fairified_value.replace(day=1)
                return anonymized_value, True
            else:
                # no need to anonymize the date
                return fairified_value, False
        else:
            return fairified_value, False
