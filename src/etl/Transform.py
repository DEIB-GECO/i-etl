import json
import re
from datetime import datetime
from typing import Any

import ujson
from pandas import DataFrame

from constants.defaults import BATCH_SIZE, PATTERN_VALUE_DIMENSION
from constants.idColumns import NO_ID, ID_COLUMNS
from database.Counter import Counter
from database.Database import Database
from database.Execution import Execution
from datatypes.Identifier import Identifier
from datatypes.OntologyResource import OntologyResource
from entities.ClinicalFeature import ClinicalFeature
from entities.ClinicalRecord import ClinicalRecord
from entities.DiagnosisFeature import DiagnosisFeature
from entities.DiagnosisRecord import DiagnosisRecord
from entities.GenomicFeature import GenomicFeature
from entities.GenomicRecord import GenomicRecord
from entities.Hospital import Hospital
from entities.ImagingFeature import ImagingFeature
from entities.ImagingRecord import ImagingRecord
from entities.MedicineFeature import MedicineFeature
from entities.MedicineRecord import MedicineRecord
from entities.Patient import Patient
from entities.PhenotypicFeature import PhenotypicFeature
from entities.PhenotypicRecord import PhenotypicRecord
from enums.DataTypes import DataTypes
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.TrueFalse import TrueFalse
from enums.Visibility import Visibility
from etl.Task import Task
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.assertion_utils import is_not_nan
from utils.cast_utils import cast_str_to_boolean, cast_str_to_datetime, cast_str_to_float, cast_str_to_int
from utils.file_utils import write_in_file
from utils.setup_logger import log


class Transform(Task):

    def __init__(self, database: Database, execution: Execution, data: DataFrame, metadata: DataFrame,
                 mapping_categorical_value_to_onto_resource: dict,
                 mapping_column_to_categorical_value: dict,
                 mapping_column_to_dimension: dict, patient_ids_mapping: dict,
                 profile: str, dataset_number: int, file_counter: int,
                 quality_stats: QualityStatistics, time_stats: TimeStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats, time_stats=time_stats)
        self.counter = Counter()  # resource counter
        self.profile = profile
        self.dataset_number = dataset_number  # file number (for each dataset)
        self.file_counter = file_counter  # file counter (for all the files created for a single dataset)

        # get data, metadata and the mapped values computed in the Extract step
        self.data = data
        self.metadata = metadata
        self.mapping_column_to_dimension = mapping_column_to_dimension
        self.mapping_categorical_value_to_onto_resource = mapping_categorical_value_to_onto_resource
        self.mapping_column_to_categorical_value = mapping_column_to_categorical_value
        # to keep track of anonymized vs. hospital patient ids
        # this is empty if no file as been provided by the user, otherwise it contains some mappings <patient ID, anonymized ID>
        self.patient_ids_mapping = patient_ids_mapping

        # to record objects that will be further inserted in the database
        self.features = []
        self.records = []
        self.hospitals = []
        self.patients = []
        self.samples = []

    def run(self) -> None:
        log.info("********** create patients")
        self.set_resource_counter_id()
        self.create_patients()

        self.set_resource_counter_id()
        log.info(f"********** create {self.profile} features and records")
        if self.profile == Profile.PHENOTYPIC:
            self.create_phenotypic_features()
            self.create_phenotypic_records()
        elif self.profile == Profile.CLINICAL:
            self.create_clinical_features()
            self.create_clinical_records()
        elif self.profile == Profile.DIAGNOSIS:
            self.create_diagnosis_features()
            self.create_diagnosis_records()
        elif self.profile == Profile.MEDICINE:
            self.create_medicine_features()
            self.create_medicine_records()
        elif self.profile == Profile.IMAGING:
            self.create_imaging_features()
            self.create_imaging_records()
        elif self.profile == Profile.GENOMIC:
            self.create_genomic_features()
            self.create_genomic_records()
        else:
            raise TypeError(f"The current file profile '{self.profile}' is unknown.")

    ##############################################################
    # FEATURES
    ##############################################################

    def create_features(self, table_name: str) -> None:
        # 1. get existing features in memory
        result = self.database.find_operation(table_name=table_name, filter_dict={}, projection={"ontology_resource.label": 1})
        db_existing_features = [res["ontology_resource"]["label"] if "ontology_resource" in res and "label" in res["ontology_resource"] else None for res in result]

        # 2. create non-existing features in-memory, then insert them
        log.info(f"create Feature instances of type {table_name} in memory")
        columns = self.metadata.columns
        for row in self.metadata.itertuples(index=False):
            column_name = row[columns.get_loc(MetadataColumns.COLUMN_NAME)]
            # columns to remove have already been removed in the Extract part from the metadata
            # here, we need to ensure that we create Features only for still-existing columns and not for ID column
            if column_name not in (ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT], ID_COLUMNS[self.execution.hospital_name][TableNames.CLINICAL_RECORD]):
                if column_name not in db_existing_features:
                    # we create a new Feature from scratch
                    new_feature = None
                    onto_resource = self.create_ontology_resource_from_row(column_name=column_name)
                    data_type = row[columns.get_loc(MetadataColumns.ETL_TYPE)]  # this has been normalized while loading + we take ETL_type to get the narrowest type (in which we cast values)
                    visibility = row[columns.get_loc(MetadataColumns.VISIBILITY)]  # this has been normalized while loading
                    dimension = self.mapping_column_to_dimension[column_name] if column_name in self.mapping_column_to_dimension else None  # else covers: there is no dataType for this column; there is no datatype in that type of entity
                    categorical_values = None
                    if column_name in self.mapping_column_to_categorical_value:
                        if data_type in [DataTypes.CATEGORY, DataTypes.REGEX]:
                            # for categorical values, we first need to take the list of (normalized) values that are available for the current column, and then take their CC
                            # this avoids to add categorical values for boolean features (where Yes and No and encoded with ontology resource), we do not add them
                            normalized_categorical_values = self.mapping_column_to_categorical_value[column_name]
                            categorical_values = [self.mapping_categorical_value_to_onto_resource[normalized_categorical_value] for normalized_categorical_value in normalized_categorical_values]
                    if table_name == TableNames.PHENOTYPIC_FEATURE:
                        new_feature = PhenotypicFeature(id_value=NO_ID,
                                                        name=column_name,
                                                        ontology_resource=onto_resource,
                                                        permitted_datatype=data_type, dimension=dimension,
                                                        counter=self.counter,
                                                        hospital_name=self.execution.hospital_name,
                                                        categories=categorical_values,
                                                        visibility=visibility)
                    elif table_name == TableNames.CLINICAL_FEATURE:
                        new_feature = ClinicalFeature(id_value=NO_ID,
                                                      name=column_name, ontology_resource=onto_resource,
                                                      permitted_datatype=data_type, dimension=dimension,
                                                      counter=self.counter,
                                                      hospital_name=self.execution.hospital_name,
                                                      categories=categorical_values,
                                                      visibility=visibility)
                    elif table_name == TableNames.DIAGNOSIS_FEATURE:
                        new_feature = DiagnosisFeature(id_value=NO_ID, name=column_name,
                                                       ontology_resource=onto_resource,
                                                       permitted_datatype=data_type,
                                                       dimension=dimension, counter=self.counter,
                                                       hospital_name=self.execution.hospital_name,
                                                       categories=categorical_values, visibility=visibility)
                    elif table_name == TableNames.GENOMIC_FEATURE:
                        new_feature = GenomicFeature(id_value=NO_ID, name=column_name,
                                                     ontology_resource=onto_resource,
                                                     permitted_datatype=data_type,
                                                     dimension=dimension, counter=self.counter,
                                                     hospital_name=self.execution.hospital_name,
                                                     categories=categorical_values, visibility=visibility)
                    elif table_name == TableNames.IMAGING_FEATURE:
                        new_feature = ImagingFeature(id_value=NO_ID, name=column_name,
                                                     ontology_resource=onto_resource,
                                                     permitted_datatype=data_type,
                                                     dimension=dimension, counter=self.counter,
                                                     hospital_name=self.execution.hospital_name,
                                                     categories=categorical_values, visibility=visibility)
                    elif table_name == TableNames.MEDICINE_FEATURE:
                        new_feature = MedicineFeature(id_value=NO_ID, name=column_name,
                                                      ontology_resource=onto_resource,
                                                      permitted_datatype=data_type,
                                                      dimension=dimension, counter=self.counter,
                                                      hospital_name=self.execution.hospital_name,
                                                      categories=categorical_values, visibility=visibility)
                    else:
                        raise NotImplementedError("To be implemented")

                    if onto_resource is not None:
                        log.info(f"adding a new {table_name} instance about {onto_resource.label}: {new_feature}")
                    else:
                        # no associated ontology code or failed to retrieve the code with API
                        log.info(f"adding a new {table_name} instance about {column_name}: {new_feature}")

                    self.features.append(new_feature)  # this cannot be null, otherwise we would have raise the above exception
                    if len(self.features) >= BATCH_SIZE:
                        write_in_file(resource_list=self.features,
                                      current_working_dir=self.execution.working_dir_current,
                                      table_name=table_name,
                                      dataset_number=self.dataset_number,
                                      file_counter=self.file_counter)
                        self.features.clear()
                        self.file_counter += 1
                else:
                    # the PhenFeature already exists, so no need to add it to the database again.
                    log.error(f"The phen feature about {column_name} already exists. Not added.")
            else:
                log.debug(f"I am skipping column {column_name} because it has been dropped or is an ID column.")
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        if len(self.features) > 0:
            write_in_file(resource_list=self.features, current_working_dir=self.execution.working_dir_current,
                          table_name=table_name, dataset_number=self.dataset_number, file_counter=self.file_counter)
            self.file_counter += 1
        self.database.load_json_in_table(table_name=table_name, unique_variables=["name"], dataset_number=self.dataset_number)

    def create_phenotypic_features(self) -> None:
        self.create_features(table_name=TableNames.PHENOTYPIC_FEATURE)

    def create_clinical_features(self) -> None:
        self.create_features(table_name=TableNames.CLINICAL_FEATURE)

    def create_diagnosis_features(self):
        self.create_features(table_name=TableNames.DIAGNOSIS_FEATURE)

    def create_medicine_features(self):
        self.create_features(table_name=TableNames.MEDICINE_FEATURE)

    def create_imaging_features(self):
        self.create_features(table_name=TableNames.IMAGING_FEATURE)

    def create_genomic_features(self):
        self.create_features(table_name=TableNames.GENOMIC_FEATURE)

    ##############################################################
    # RECORDS
    ##############################################################

    def create_records(self, table_name: str) -> None:
        log.info(f"create {table_name} instances in memory")

        # a. load some data from the database to compute references
        mapping_hospital_to_hospital_id = self.database.retrieve_mapping(table_name=TableNames.HOSPITAL,
                                                                         key_fields="name", value_fields="identifier")
        feature_table_name = TableNames.get_feature_table_from_record_table(record_table_name=table_name)
        mapping_column_to_feature_id = self.database.retrieve_mapping(table_name=feature_table_name,
                                                                      key_fields="name",
                                                                      value_fields="identifier")
        log.info(f"{len(mapping_column_to_feature_id)} {feature_table_name} have been retrieved from the database.")
        log.info(mapping_column_to_feature_id)

        # b. Create Record instance, and write them in temporary (JSON) files
        columns = self.data.columns
        for row in self.data.itertuples(index=False):
            # create Record instances by associating observations to a patient, a record and a hospital
            for column_name in columns:
                value = row[columns.get_loc(column_name)]
                # log.debug(f"for row {row}) and column {column_name} (type: {type(column_name)}), value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that Feature, no need to create a Record instance
                    # log.error(f"skipping value {value} in column {column_name} because it is None, or empty or nan")
                    self.quality_stats.count_empty_cell_for_column(column_name=column_name)
                else:
                    # log.info(row)
                    if column_name in mapping_column_to_feature_id:
                        # we know a code for this column, so we can register the value of that Feature in a new Record
                        feature_id = Identifier(value=mapping_column_to_feature_id[column_name])
                        hospital_id = Identifier(value=mapping_hospital_to_hospital_id[self.execution.hospital_name])
                        # get the anonymized patient id using the mapping <initial id, anonymized id>
                        patient_id = Identifier(value=self.patient_ids_mapping[row[columns.get_loc(ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT])]])
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        anonymized_value, is_anonymized = self.anonymize_value(column_name=column_name,
                                                                               fairified_value=fairified_value)
                        if is_anonymized:
                            fairified_value = anonymized_value  # we coule anonymize this value, this is the one to insert in the DB
                        dataset_name = self.execution.current_filepath
                        if table_name == TableNames.PHENOTYPIC_RECORD:
                            new_record = PhenotypicRecord(id_value=NO_ID, feature_id=feature_id,
                                                          patient_id=patient_id, hospital_id=hospital_id,
                                                          value=fairified_value,
                                                          counter=self.counter,
                                                          hospital_name=self.execution.hospital_name,
                                                          dataset_name=dataset_name)
                        elif table_name == TableNames.CLINICAL_RECORD:
                            if TableNames.CLINICAL_RECORD in ID_COLUMNS[self.execution.hospital_name] and ID_COLUMNS[self.execution.hospital_name][TableNames.CLINICAL_RECORD] in row:
                                # this dataset contains a sample bar code (or equivalent)
                                base_id = row[columns.get_loc(ID_COLUMNS[self.execution.hospital_name][TableNames.CLINICAL_RECORD])]
                            else:
                                base_id = None
                            new_record = ClinicalRecord(id_value=NO_ID, feature_id=feature_id, patient_id=patient_id,
                                                        hospital_id=hospital_id, value=fairified_value,
                                                        base_id=base_id,
                                                        counter=self.counter, hospital_name=self.execution.hospital_name,
                                                        dataset_name=dataset_name)
                            # log.info(f"new clinical record: {new_record}")
                        elif table_name == TableNames.DIAGNOSIS_RECORD:
                            new_record = DiagnosisRecord(id_value=NO_ID, feature_id=feature_id,
                                                         patient_id=patient_id, hospital_id=hospital_id,
                                                         value=fairified_value,
                                                         counter=self.counter,
                                                         hospital_name=self.execution.hospital_name,
                                                         dataset_name=dataset_name)
                        elif table_name == TableNames.GENOMIC_RECORD:
                            new_record = GenomicRecord(id_value=NO_ID, feature_id=feature_id,
                                                       patient_id=patient_id, hospital_id=hospital_id,
                                                       vcf=None,
                                                       value=fairified_value,
                                                       counter=self.counter,
                                                       hospital_name=self.execution.hospital_name,
                                                       dataset_name=dataset_name)
                        elif table_name == TableNames.IMAGING_RECORD:
                            new_record = ImagingRecord(id_value=NO_ID, feature_id=feature_id,
                                                       patient_id=patient_id, hospital_id=hospital_id,
                                                       scan=None,
                                                       value=fairified_value,
                                                       counter=self.counter,
                                                       hospital_name=self.execution.hospital_name,
                                                       dataset_name=dataset_name)
                        elif table_name == TableNames.MEDICINE_RECORD:
                            new_record = MedicineRecord(id_value=NO_ID, feature_id=feature_id,
                                                        patient_id=patient_id, hospital_id=hospital_id,
                                                        value=fairified_value,
                                                        counter=self.counter,
                                                        hospital_name=self.execution.hospital_name,
                                                        dataset_name=dataset_name)
                        else:
                            raise NotImplementedError("Not implemented yet.")
                        self.records.append(new_record)
                        if len(self.records) >= BATCH_SIZE:
                            log.info(f"writing {len(self.records)} in file")
                            write_in_file(resource_list=self.records,
                                          current_working_dir=self.execution.working_dir_current,
                                          table_name=table_name, dataset_number=self.dataset_number,
                                          file_counter=self.file_counter)
                            self.records.clear()
                            self.file_counter += 1
                    else:
                        # this represents the case when a column has not been converted to a Feature resource
                        # this may happen for ID column for instance, or in BUZZI many clinical columns are not described in the metadata, thus skipped here
                        # log.error(f"Skipping column {column_name} for row {index}")
                        pass
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        if len(self.records) > 0:
            write_in_file(resource_list=self.records, current_working_dir=self.execution.working_dir_current,
                          table_name=table_name, dataset_number=self.dataset_number, file_counter=self.file_counter)
            self.file_counter += 1

    def create_phenotypic_records(self) -> None:
        self.create_records(table_name=TableNames.PHENOTYPIC_RECORD)

    def create_clinical_records(self) -> None:
        self.create_records(table_name=TableNames.CLINICAL_RECORD)

    def create_diagnosis_records(self):
        self.create_records(table_name=TableNames.DIAGNOSIS_RECORD)

    def create_medicine_records(self):
        self.create_records(table_name=TableNames.MEDICINE_RECORD)

    def create_imaging_records(self):
        self.create_records(table_name=TableNames.IMAGING_RECORD)

    def create_genomic_records(self):
        self.create_records(table_name=TableNames.GENOMIC_RECORD)

    ##############################################################
    # OTHER ENTITIES
    ##############################################################

    def create_patients(self) -> None:
        # if provided as input, load the mapping between patient IDs and anonymized IDs
        self.load_patient_id_mapping()

        log.info(f"create patient instances in memory")
        columns = self.data.columns
        if ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT] in self.data.columns:
            log.info(f"creating patients using column {ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]}")
            for row in self.data.itertuples(index=False):
                row_patient_id = row[columns.get_loc(ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT])]
                if row_patient_id not in self.patient_ids_mapping:
                    # the (anonymized) patient does not exist yet, we will create it
                    new_patient = Patient(id_value=NO_ID, counter=self.counter, hospital_name=self.execution.hospital_name)
                    # log.info(f"create new patient {row_patient_id} with anonymized ID {new_patient.identifier.value}")
                    self.patient_ids_mapping[row_patient_id] = new_patient.identifier.value  # keep track of anonymized patient ids
                else:
                    # the (anonymized) patient id already exists, we take it from the mapping
                    # log.info(f"create patient {row_patient_id} with existing anonymized ID {self.patient_ids_mapping[row_patient_id]}")
                    new_patient = Patient(id_value=self.patient_ids_mapping[row_patient_id], counter=self.counter,
                                          hospital_name=self.execution.hospital_name)
                self.patients.append(new_patient)
                if len(self.patients) >= BATCH_SIZE:
                    # this will save the data if it has reached BATCH_SIZE
                    write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current,
                                  table_name=TableNames.PATIENT,
                                  dataset_number=self.dataset_number, file_counter=self.file_counter)
                    self.patients = []
                    self.file_counter += 1
                    # no need to load Patient instances because they are referenced using their ID,
                    # which was provided by the hospital (thus is known by the dataset)
            if len(self.patients) > 0:
                write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current,
                                table_name=TableNames.PATIENT, dataset_number=self.dataset_number, file_counter=self.file_counter)
                self.file_counter += 1
            # finally, we also write the mapping patient ID / anonymized ID in a file - this will be ingested for subsequent runs to not renumber existing anonymized patients
            with open(self.execution.anonymized_patient_ids_filepath, "w") as data_file:
                try:
                    ujson.dump(self.patient_ids_mapping, data_file)
                except Exception:
                    raise ValueError(
                        f"Could not dump the {len(self.patient_ids_mapping)} JSON resources in the file located at {self.execution.anonymized_patient_ids_filepath}.")
            self.database.load_json_in_table(table_name=TableNames.PATIENT, unique_variables=["identifier"], dataset_number=self.dataset_number)
        else:
            # no patient ID in this dataset
            log.error(f"The column {ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]} has been declared as the patient id but has not been found in the data.")
            pass
    ##############################################################
    # UTILITIES
    ##############################################################

    def set_resource_counter_id(self) -> None:
        self.counter.set_with_database(database=self.database)

    def create_hospital(self) -> None:
        log.info(f"create hospital instance in memory")
        new_hospital = Hospital(id_value=NO_ID, name=self.execution.hospital_name, counter=self.counter)
        self.hospitals.append(new_hospital)
        write_in_file(resource_list=self.hospitals, current_working_dir=self.execution.working_dir_current,
                      table_name=TableNames.HOSPITAL, dataset_number=self.dataset_number, file_counter=self.file_counter)
        self.file_counter += 1
        self.database.load_json_in_table(table_name=TableNames.HOSPITAL, unique_variables=["name"], dataset_number=self.dataset_number)

    def load_patient_id_mapping(self) -> None:
        log.info(f"Patient ID mapping filepath is {self.execution.anonymized_patient_ids_filepath}")

        # index_col is False to not add a column with line numbers
        self.patient_ids_mapping = {}
        log.debug(self.execution.anonymized_patient_ids_filepath)
        if self.execution.anonymized_patient_ids_filepath is not None:
            with open(self.execution.anonymized_patient_ids_filepath, "r") as f:
                self.patient_ids_mapping = ujson.load(f)
        log.info(f"{len(self.patient_ids_mapping)} patient IDs in the mapping file.")

    def create_ontology_resource_from_row(self, column_name: str) -> OntologyResource | None:
        rows = self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name]
        if len(rows) == 1:
            row = rows.iloc[0]
            onto_name = row[self.metadata.columns.get_loc(MetadataColumns.ONTO_NAME)]
            if is_not_nan(onto_name):
                onto_resource = OntologyResource(
                    ontology=Ontologies.get_enum_from_name(onto_name),
                    full_code=row[self.metadata.columns.get_loc(MetadataColumns.ONTO_CODE)],
                    quality_stats=self.quality_stats,
                    label=None)
                return onto_resource
            else:
                log.error("Did not find the column '%s' in the metadata", column_name)
                return None
        elif len(rows) == 0:
            log.error("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            log.error("Found several times the column '%s' in the metadata", column_name)
            return None

    def fairify_value(self, column_name: str, value: Any) -> str | float | datetime | OntologyResource:
        current_column_info = DataFrame(self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name])
        etl_type = current_column_info[MetadataColumns.ETL_TYPE].iloc[0]  # we need to take the value itself, otherwise we end up with a series of 1 element (the ETL type)
        the_normalized_value = MetadataColumns.normalize_value(column_value=value)  # we compute only once the cast value and return it whenever we cannot FAIRify deeply the value
        return_value = None  # to ease logging, we also save the return value in a variable and return it at the very end
        expected_unit = self.mapping_column_to_dimension[column_name] if column_name in self.mapping_column_to_dimension else None  # there was some unit specified in the metadata or extracted from the data

        # log.debug(f"ETL type is {etl_type}, expected unit is {expected_unit}")
        if etl_type == DataTypes.STRING:
            return value  # the value is already normalized, we can return it as is
        elif etl_type == DataTypes.CATEGORY:
            # we look for the CC associated to that categorical value
            # we need to check that (a) the column expects this categorical value and (b) this categorical has an associated CC
            if column_name in self.mapping_column_to_categorical_value and value in self.mapping_column_to_categorical_value[column_name] and value in self.mapping_categorical_value_to_onto_resource:
                return_value = self.mapping_categorical_value_to_onto_resource[value]
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
            if column_name in self.mapping_column_to_categorical_value and value in \
                    self.mapping_column_to_categorical_value[
                        column_name] and value in self.mapping_categorical_value_to_onto_resource:
                cc = self.mapping_categorical_value_to_onto_resource[value]
                # this should rather be a boolean, let's cast it as boolean, instead of using Yes/No SNOMED_CT codes
                if cc == TrueFalse.TRUE:
                    return_value = True
                elif cc == TrueFalse.FALSE:
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
                        self.quality_stats.add_numerical_value_with_unmatched_dimension(column_name=column_name,
                                                                                        expected_dimension=expected_unit,
                                                                                        current_dimension=unit,
                                                                                        value=value)
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
                or (etl_type == DataTypes.CATEGORY and not isinstance(return_value, OntologyResource))):
            self.quality_stats.add_column_with_unmatched_typeof_etl_types(column_name=column_name,
                                                                          typeof_type=type(return_value).__name__,
                                                                          etl_type=etl_type)

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
        etl_type = current_column_info[MetadataColumns.ETL_TYPE].iloc[
            0]  # we need to take the value itself, otherwise we end up with a series of 1 element (the ETL type)
        visibility = current_column_info[MetadataColumns.VISIBILITY].iloc[0]
        if etl_type == DataTypes.DATETIME:
            if visibility == Visibility.ANONYMIZED:
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
            if visibility == Visibility.ANONYMIZED:
                # anonymize the date
                anonymized_value = fairified_value.replace(day=1)
                return anonymized_value, True
            else:
                # no need to anonymize the date
                return fairified_value, False
        else:
            return fairified_value, False
