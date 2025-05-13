import re
from datetime import datetime
from itertools import islice
from typing import Any

import pandas as pd
import ujson
from pandas import DataFrame

from constants.defaults import BATCH_SIZE, PATTERN_VALUE_UNIT, NO_ID
from database.Counter import Counter
from database.Database import Database
from entities.Dataset import Dataset
from database.Execution import Execution
from entities.ClinicalFeature import ClinicalFeature
from entities.ClinicalRecord import ClinicalRecord
from entities.DiagnosisFeature import DiagnosisFeature
from entities.DiagnosisRecord import DiagnosisRecord
from entities.Feature import Feature
from entities.GenomicFeature import GenomicFeature
from entities.GenomicRecord import GenomicRecord
from entities.Hospital import Hospital
from entities.ImagingFeature import ImagingFeature
from entities.ImagingRecord import ImagingRecord
from entities.MedicineFeature import MedicineFeature
from entities.MedicineRecord import MedicineRecord
from entities.OntologyResource import OntologyResource
from entities.Patient import Patient
from entities.PhenotypicFeature import PhenotypicFeature
from entities.PhenotypicRecord import PhenotypicRecord
from entities.Resource import Resource
from enums.DataTypes import DataTypes
from enums.DiagnosisColumns import DiagnosisColumns
from enums.Domain import Domain
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.TimerKeys import TimerKeys
from enums.Visibility import Visibility
from etl.Task import Task
from src.constants.defaults import DEFAULT_NAN_VALUE
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.cast_utils import cast_str_to_boolean, cast_str_to_datetime, cast_str_to_float, cast_str_to_int
from utils.file_utils import write_in_file
from utils.setup_logger import log


class Transform(Task):

    def __init__(self, database: Database, execution: Execution, data: DataFrame, metadata: DataFrame,
                 mapping_column_to_categorical_value: dict,
                 mapping_column_to_unit: dict, mapping_column_to_domain: dict,
                 mapping_column_to_type: dict | None,
                 profile: str, dataset_id: int, dataset_key: Dataset, load_patients: bool,
                 quality_stats: QualityStatistics):
        super().__init__(database=database, execution=execution, quality_stats=quality_stats)
        self.time_statistics = TimeStatistics(record_stats=True)
        self.counter = Counter()  # resource counter
        self.profile = profile
        self.load_patients = load_patients
        self.dataset_id = dataset_id  # file number (one for each dataset)
        self.dataset_instance = dataset_key

        # get data, metadata and the mapped values computed in the Extract step
        self.data = data
        self.metadata = metadata
        self.mapping_column_to_unit = mapping_column_to_unit
        self.mapping_column_to_categorical_value = mapping_column_to_categorical_value
        if mapping_column_to_type is not None:
            self.mapping_column_to_type = mapping_column_to_type  # we give it to Transform during tests
        else:
            self.mapping_column_to_type = {}
        self.mapping_column_to_visibility = {}
        self.mapping_column_to_domain = mapping_column_to_domain
        self.mapping_apivalue_to_onto_resource = {}  # for API columns only; of the form: <"onto_name:onto_code": onto resource>
        # to keep track of anonymized vs. hospital patient ids
        # this is empty if no file as been provided by the user, otherwise it contains some mappings <patient ID, anonymized ID>
        self.patient_ids_mapping = {}
        # to keep track of the total number of values that could exist (whether they are Nan or a real value)
        self.mapping_column_all_count = {}

        # to record objects that will be further inserted in the database
        self.features = []
        self.records = []
        self.hospitals = []
        self.patients = []
        self.samples = []

    def run(self) -> None:
        self.load_patient_id_mapping()  # we always load the mapping in order to retrieve existing identifiers when creating data for existing patients
        if self.load_patients:
            # this is the first profile of the dataset, we load patients
            log.info("********** create patients")
            self.counter.set_with_database(database=self.database)
            self.create_patients()
        else:
            # this is another profile of the same dataset, we do not reload the same patients
            pass

        log.info(f"********** create {self.profile} features and records")
        self.counter.set_with_database(database=self.database)
        self.create_features()
        self.counter.set_with_database(database=self.database)
        self.create_records()

    ##############################################################
    # FEATURES
    ##############################################################

    def create_features(self) -> None:
        # 1. get existing features in memory
        log.info(f"Retrieving {self.profile}Feature from database")
        result = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={Resource.ENTITY_TYPE_: f"{self.profile}Feature"}, projection={Feature.NAME_: 1, Resource.IDENTIFIER_: 1, Resource.DATASET_: 1})
        db_existing_features = [res[Feature.NAME_] for res in result]  # list of features (names) existing in this dataset
        log.info(db_existing_features)

        # 2. create non-existing features in-memory, then insert them
        log.info(f"Creating {self.profile}Feature instances in memory")
        columns = self.metadata.columns
        for row in self.metadata.itertuples(index=False):
            column_name = row[columns.get_loc(MetadataColumns.COLUMN_NAME)]
            # columns to remove have already been removed in the Extract part from the metadata
            # here, we need to ensure that we create Features that have a name, which are not IDs (patient or sample) nor for Diagnosis counter (clinical base_id)
            if column_name not in ["", self.execution.patient_id_column_name, self.execution.sample_id_column_name, DiagnosisColumns.DISEASE_COUNTER]:
                if column_name not in db_existing_features:
                    # we create a new Feature from scratch
                    onto_resource = self.create_ontology_resource_from_row(column_name=column_name)
                    data_type = row[columns.get_loc(MetadataColumns.ETL_TYPE)]  # this has been normalized while loading + we take ETL_type to get the narrowest type (in which we cast values)
                    if data_type == DataTypes.API:  # because the catalogue does not know about the API datatype; each API call leads to an ontology resource, thus a category
                        data_type = DataTypes.CATEGORY
                    self.mapping_column_to_type[column_name] = data_type
                    visibility = row[columns.get_loc(MetadataColumns.VISIBILITY)]  # this has been normalized while loading
                    self.mapping_column_to_visibility[column_name] = visibility
                    unit = self.mapping_column_to_unit[column_name] if column_name in self.mapping_column_to_unit else None  # else covers: there is no dataType for this column; there is no datatype in that type of entity
                    description = row[columns.get_loc(MetadataColumns.SIGNIFICATION_EN)]
                    normalized_categorical_values = None
                    domain = {}
                    if data_type in [DataTypes.CATEGORY, DataTypes.REGEX] and column_name in self.mapping_column_to_categorical_value:
                        # for categorical values, we first need to take the list of (normalized) values that are available for the current column, and then take their CC
                        # this avoids to add categorical values for boolean features (where Yes and No and encoded with ontology resource), we do not add them
                        normalized_categorical_values = list(self.mapping_column_to_categorical_value[column_name].values())
                        log.info(normalized_categorical_values)
                        # categorical_values = [self.mapping_categorical_value_to_onto_resource[normalized_categorical_value] for normalized_categorical_value in normalized_categorical_values]
                        domain[Domain.ACCEPTED_VALUES] = list(self.mapping_column_to_categorical_value[column_name].keys())
                        log.info(domain[Domain.ACCEPTED_VALUES])
                    elif data_type in [DataTypes.DATE, DataTypes.DATETIME] or data_type in DataTypes.numeric():
                        if column_name in self.mapping_column_to_domain and self.mapping_column_to_domain[column_name] is not None:
                            if Domain.MIN in self.mapping_column_to_domain[column_name]:
                                domain[Domain.MIN] = self.mapping_column_to_domain[column_name][Domain.MIN]
                            if Domain.MAX in self.mapping_column_to_domain[column_name]:
                                domain[Domain.MAX] = self.mapping_column_to_domain[column_name][Domain.MAX]
                    if self.profile == Profile.PHENOTYPIC:
                        new_feature = PhenotypicFeature(identifier=NO_ID, name=column_name,
                                                        ontology_resource=onto_resource,
                                                        data_type=data_type, unit=unit,
                                                        counter=self.counter,
                                                        categories=normalized_categorical_values,
                                                        visibility=visibility,
                                                        dataset=self.dataset_instance.global_identifier,
                                                        description=description,
                                                        domain=domain)
                    elif self.profile == Profile.CLINICAL:
                        new_feature = ClinicalFeature(identifier=NO_ID, name=column_name, ontology_resource=onto_resource,
                                                      data_type=data_type, unit=unit,
                                                      counter=self.counter,
                                                      categories=normalized_categorical_values,
                                                      visibility=visibility,
                                                      dataset=self.dataset_instance.global_identifier,
                                                      description=description,
                                                      domain=domain)
                    elif self.profile == Profile.DIAGNOSIS:
                        new_feature = DiagnosisFeature(identifier=NO_ID, name=column_name,
                                                       ontology_resource=onto_resource,
                                                       data_type=data_type,
                                                       unit=unit, counter=self.counter,
                                                       categories=normalized_categorical_values,
                                                       visibility=visibility,
                                                       dataset=self.dataset_instance.global_identifier,
                                                       description=description,
                                                       domain=domain)
                    elif self.profile == Profile.GENOMIC:
                        new_feature = GenomicFeature(identifier=NO_ID, name=column_name,
                                                     ontology_resource=onto_resource,
                                                     data_type=data_type,
                                                     unit=unit, counter=self.counter,
                                                     categories=normalized_categorical_values,
                                                     visibility=visibility,
                                                     dataset=self.dataset_instance.global_identifier,
                                                     description=description,
                                                     domain=domain)
                    elif self.profile == Profile.IMAGING:
                        new_feature = ImagingFeature(identifier=NO_ID, name=column_name,
                                                     ontology_resource=onto_resource,
                                                     data_type=data_type,
                                                     unit=unit, counter=self.counter,
                                                     categories=normalized_categorical_values,
                                                     visibility=visibility,
                                                     dataset=self.dataset_instance.global_identifier,
                                                     description=description,
                                                     domain=domain)
                    elif self.profile == Profile.MEDICINE:
                        new_feature = MedicineFeature(identifier=NO_ID, name=column_name,
                                                      ontology_resource=onto_resource,
                                                      data_type=data_type,
                                                      unit=unit, counter=self.counter,
                                                      categories=normalized_categorical_values,
                                                      visibility=visibility,
                                                      dataset=self.dataset_instance.global_identifier,
                                                      description=description,
                                                      domain=domain)
                    else:
                        raise NotImplementedError("To be implemented")

                    if onto_resource is not None:
                        log.info(f"adding a new {self.profile} feature about {onto_resource.label}: {new_feature}")
                    else:
                        # no associated ontology code or failed to retrieve the code with API
                        log.info(f"adding a new {self.profile} feature about {column_name}: {new_feature}")

                    self.features.append(new_feature.to_json())  # this cannot be null, otherwise we would have raise the above exception
                    if len(self.features) >= BATCH_SIZE:
                        self.process_batch_of_features()
                else:
                    # the Feature already exists, so no need to add it to the database again.
                    # however, we need to update the set of datasets in which it appears
                    log.error(f"The feature about {column_name} already exists. Not added.")
            else:
                log.debug(f"I am skipping column {column_name} because it has been dropped or is an ID column or its name is empty.")
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        if len(self.features) > 0:
            self.process_batch_of_features()
        # write all the features in the database now (to be able to retrieve them for creating records just after
        self.database.load_json_in_table(table_name=TableNames.FEATURE, unique_variables=[Feature.NAME_], dataset_id=self.dataset_id)

    ##############################################################
    # RECORDS
    ##############################################################

    def create_records(self) -> None:
        log.info(f"creating {self.profile}Record instances in memory")

        # a. load some data from the database to compute references
        mapping_hospital_to_hospital_id = self.database.retrieve_mapping(table_name=TableNames.HOSPITAL,
                                                                         key_fields=Hospital.NAME_,
                                                                         value_fields=Resource.IDENTIFIER_,
                                                                         filter_dict={})
        log.info(mapping_hospital_to_hospital_id)
        mapping_column_to_feature_id = self.database.retrieve_mapping(table_name=TableNames.FEATURE,
                                                                      key_fields=Feature.NAME_,
                                                                      value_fields=Resource.IDENTIFIER_,
                                                                      filter_dict={Resource.ENTITY_TYPE_: f"{self.profile}{TableNames.FEATURE}"})
        log.info(f"{len(mapping_column_to_feature_id)} {self.profile}{TableNames.FEATURE} have been retrieved from the database.")
        # if len(mapping_column_to_feature_id) > 10:
        #     # print only the ten first elements
        #     log.info(dict(islice(mapping_column_to_feature_id.items(), 10)))
        #
        # else:
        # log.info(mapping_column_to_feature_id)

        # b. Create Record instance, and write them in temporary (JSON) files
        columns = self.data.columns
        for row in self.data.itertuples(index=False):
            # log.info(row)
            # create Record instances by associating observations to a patient, a record and a hospital
            for column_name in columns:
                value = row[columns.get_loc(column_name)]
                # log.debug(f"column {column_name} (type: {type(column_name)}), value is {value}")
                if value == "":
                    # if there is no value for that Feature, no need to create a Record instance
                    # log.error(f"skipping value {value} in column {column_name} because it is None, or empty or nan")
                    self.quality_stats.count_empty_cell_for_column(column_name=column_name)
                    if column_name in mapping_column_to_feature_id:
                        feature_id = mapping_column_to_feature_id[column_name]
                        if feature_id not in self.mapping_column_all_count:
                            self.mapping_column_all_count[feature_id] = 1
                        else:
                            self.mapping_column_all_count[feature_id] = self.mapping_column_all_count[feature_id] + 1
                else:
                    if column_name in mapping_column_to_feature_id:
                        # we know a code for this column, so we can register the value of that Feature in a new Record
                        feature_id = mapping_column_to_feature_id[column_name]
                        hospital_id = mapping_hospital_to_hospital_id[self.execution.hospital_name]
                        # get the anonymized patient id using the mapping <initial id, anonymized id>
                        patient_id = self.patient_ids_mapping[row[columns.get_loc(self.execution.patient_id_column_name)]]
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        anonymized_value, is_anonymized = self.anonymize_value(column_name=column_name,
                                                                               fairified_value=fairified_value)
                        if is_anonymized:
                            fairified_value = anonymized_value  # we could anonymize this value, this is the one to insert in the DB
                        dataset = self.execution.current_dataset_gid
                        if self.profile == Profile.PHENOTYPIC:
                            new_record = PhenotypicRecord(identifier=NO_ID,
                                                          instantiates=feature_id,
                                                          has_subject=patient_id,
                                                          registered_by=hospital_id,
                                                          value=fairified_value,
                                                          counter=self.counter,
                                                          dataset=dataset)
                        elif self.profile == Profile.CLINICAL:
                            if self.execution.sample_id_column_name in columns:
                                # this dataset contains a sample barcode (or equivalent)
                                base_id = row[columns.get_loc(self.execution.sample_id_column_name)]
                            else:
                                base_id = None
                            new_record = ClinicalRecord(identifier=NO_ID,
                                                        instantiates=feature_id,
                                                        has_subject=patient_id,
                                                        registered_by=hospital_id,
                                                        value=fairified_value,
                                                        base_id=base_id,
                                                        counter=self.counter,
                                                        dataset=dataset)
                        elif self.profile == Profile.DIAGNOSIS:
                            if DiagnosisColumns.DISEASE_COUNTER in columns:
                                # this dataset contains a diagnosis counter because patients may be affected
                                # by several diseases
                                # we also need to force the conversion to int, because we read the data as str
                                # and such values do not go through the fairification method
                                try:
                                    diagnosis_counter = int(row[columns.get_loc(DiagnosisColumns.DISEASE_COUNTER)])
                                except:
                                    # the value is None because the patient diseases is unknown in the disease classification
                                    diagnosis_counter = None
                            else:
                                diagnosis_counter = None
                            new_record = DiagnosisRecord(identifier=NO_ID,
                                                         instantiates=feature_id,
                                                         has_subject=patient_id,
                                                         registered_by=hospital_id,
                                                         value=fairified_value,
                                                         diagnosis_counter=diagnosis_counter,
                                                         counter=self.counter,
                                                         dataset=dataset)
                        elif self.profile == Profile.GENOMIC:
                            new_record = GenomicRecord(identifier=NO_ID,
                                                       instantiates=feature_id,
                                                       has_subject=patient_id,
                                                       registered_by=hospital_id,
                                                       vcf=None,
                                                       value=fairified_value,
                                                       counter=self.counter,
                                                       dataset=dataset)
                        elif self.profile == Profile.IMAGING:
                            new_record = ImagingRecord(identifier=NO_ID,
                                                       instantiates=feature_id,
                                                       has_subject=patient_id,
                                                       registered_by=hospital_id,
                                                       scan=None,
                                                       value=fairified_value,
                                                       counter=self.counter,
                                                       dataset=dataset)
                        elif self.profile == Profile.MEDICINE:
                            new_record = MedicineRecord(identifier=NO_ID,
                                                        instantiates=feature_id,
                                                        has_subject=patient_id,
                                                        registered_by=hospital_id,
                                                        value=fairified_value,
                                                        counter=self.counter,
                                                        dataset=dataset)
                        else:
                            raise NotImplementedError("Not implemented yet.")
                        self.records.append(new_record.to_json())
                        if len(self.records) >= BATCH_SIZE:
                            self.process_batch_of_records()
                        # to compute the percentage of missing values in features' profiles
                        if feature_id not in self.mapping_column_all_count:
                            self.mapping_column_all_count[feature_id] = 1
                        else:
                            self.mapping_column_all_count[feature_id] = self.mapping_column_all_count[feature_id] + 1
                    else:
                        # this represents the case when a column has not been converted to a Feature resource
                        # this may happen for ID column for instance, or in BUZZI many clinical columns are not described in the metadata, thus skipped here
                        # log.error(f"Skipping column {column_name} for row {index}")
                        pass
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        if len(self.records) > 0:
            self.process_batch_of_records()
        # and save the total counts for each column (to compute the percentage of missing values in the profiles)
        all_counts = []  # a list of <"identifier": identifier, "all_counts": all_counts> instead of <identifier: all_counts>
        # log.info(self.mapping_column_all_count)
        for k in self.mapping_column_all_count:
            all_counts.append({Resource.IDENTIFIER_: k, "count_all_values": self.mapping_column_all_count[k], Resource.DATASET_: self.dataset_instance.global_identifier})
        if len(all_counts) > 0:
            self.database.insert_many_tuples(table_name=TableNames.COUNTS_FEATURES, tuples=all_counts)
        else:
            log.info(all_counts)
            log.error(f"No record counts")

    ##############################################################
    # OTHER ENTITIES
    ##############################################################

    def create_patients(self) -> None:
        log.info(f"create Patient instances in memory")
        log.info(self.execution.patient_id_column_name)
        if self.execution.patient_id_column_name not in self.data.columns:
            # no patient ID in this dataset
            log.error(f"The column {self.execution.patient_id_column_name} has been declared as the patient id but has not been found in the data. Creating automatically patient IDs.")
            pids = [i for i in range(1, len(self.data)+1)]
            self.data[self.execution.patient_id_column_name] = pids

        columns = self.data.columns
        log.info(columns)
        log.info(f"creating patients using column {self.execution.patient_id_column_name}, with index {columns.get_loc(self.execution.patient_id_column_name)}")
        for row in self.data.itertuples(index=False):
            row_patient_id = row[columns.get_loc(self.execution.patient_id_column_name)]
            if row_patient_id == "":
                # this line does not contain a patient id (the cell is empty)
                # thus, we skip it
                pass
            else:
                if row_patient_id not in self.patient_ids_mapping:
                    # the (anonymized) patient does not exist yet, we will create it
                    new_patient = Patient(identifier=NO_ID, counter=self.counter)
                    # log.info(f"create new patient {row_patient_id} with anonymized ID {new_patient.identifier.value}")
                    self.patient_ids_mapping[row_patient_id] = new_patient.identifier  # keep track of anonymized patient ids
                else:
                    # the (anonymized) patient id already exists, we take it from the mapping
                    # log.info(f"create patient {row_patient_id} with existing anonymized ID {self.patient_ids_mapping[row_patient_id]}")
                    new_patient = Patient(identifier=self.patient_ids_mapping[row_patient_id], counter=self.counter)
                self.patients.append(new_patient.to_json())

                self.time_statistics.start(dataset=self.dataset_instance.global_identifier, key=TimerKeys.CHECK_BATCH_SIZE_PATIENTS)
                if len(self.patients) >= BATCH_SIZE:
                    self.process_batch_of_patients()
                # no need to load Patient instances because they are referenced using their ID,
                # which was provided by the hospital (thus is known by the dataset)
        if len(self.patients) > 0:
            self.process_batch_of_patients()
        # finally, we also write the mapping patient ID / anonymized ID in a file - this will be ingested for subsequent runs to not renumber existing anonymized patients
        with open(self.execution.anonymized_patient_ids_filepath, "w") as data_file:
            try:
                ujson.dump(self.patient_ids_mapping, data_file)
            except Exception:
                raise ValueError(
                    f"Could not dump the {len(self.patient_ids_mapping)} JSON resources in the file located at {self.execution.anonymized_patient_ids_filepath}.")
        self.database.load_json_in_table(table_name=TableNames.PATIENT, unique_variables=[Resource.IDENTIFIER_], dataset_id=self.dataset_id)

    ##############################################################
    # UTILITIES
    ##############################################################

    def process_batch_of_patients(self) -> None:
        log.info(f"writing {len(self.patients)} patients in file")
        write_in_file(resource_list=self.patients,
                      current_working_dir=self.execution.working_dir_current,
                      table_name=TableNames.PATIENT,
                      is_feature=False,
                      dataset_id=self.dataset_id,
                      to_json=False)
        self.patients.clear()

    def process_batch_of_features(self) -> None:
        log.info(f"writing {len(self.features)} features in file")
        write_in_file(resource_list=self.features,
                      current_working_dir=self.execution.working_dir_current,
                      table_name=TableNames.FEATURE,
                      is_feature=True,
                      dataset_id=self.dataset_id,
                      to_json=False)
        self.features.clear()

    def process_batch_of_records(self) -> None:
        log.info(f"writing {len(self.records)} records in file")
        write_in_file(resource_list=self.records,
                      current_working_dir=self.execution.working_dir_current,
                      table_name=TableNames.RECORD,
                      is_feature=False,
                      dataset_id=self.dataset_id,
                      to_json=False)
        self.records.clear()

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
        rows = self.metadata[self.metadata[MetadataColumns.COLUMN_NAME].values == column_name]
        if len(rows) == 1:
            row = rows.iloc[0]
            onto_code = row.iloc[self.metadata.columns.get_loc(MetadataColumns.ONTO_CODE)]
            if len(onto_code) > 0:
                onto_system = Ontologies.get_enum_from_name(row.iloc[self.metadata.columns.get_loc(MetadataColumns.ONTO_NAME)])
                if type(onto_system) is dict and len(onto_system) > 0:
                    the_or = OntologyResource(system=onto_system, code=onto_code, label=None, quality_stats=self.quality_stats)
                    return the_or
                else:
                    log.error(
                        f"In the metadata, {MetadataColumns.ONTO_NAME} is empty or unrecognised for the column '{column_name}'.")
                    return None
            else:
                log.error(f"In the metadata, {MetadataColumns.ONTO_CODE} is empty for the column '{column_name}'.")
                return None
        elif len(rows) == 0:
            log.error(f"Did not find the column {column_name} in the metadata")
            return None
        else:
            log.error(f"Found several times the column {column_name} in the metadata")
            return None

    def fairify_value(self, column_name: str, value: Any) -> str | float | datetime | OntologyResource:
        if pd.isnull(value):
            # this is an explicit NaN value, we keep it as such (and do not convert it to None) in oder to insert it in mongodb
            return DEFAULT_NAN_VALUE
        else:
            return_value = None  # to ease logging, we also save the return value in a variable and return it at the very end
            etl_type = self.mapping_column_to_type[column_name] if column_name in self.mapping_column_to_type else DataTypes.STRING
            expected_unit = self.mapping_column_to_unit[column_name] if column_name in self.mapping_column_to_unit else None  # there was some unit specified in the metadata or extracted from the data

            # log.debug(f"ETL type is {etl_type}, expected unit is {expected_unit}")
            if etl_type == DataTypes.STRING:
                return value  # the value is already normalized, we can return it as is
            elif etl_type == DataTypes.LIST:
                return_value = value.split(",")
                return_value = [elem.strip() for elem in return_value]  # trim spaces around each str value
            elif etl_type == DataTypes.CATEGORY:
                # we look for the CC associated to that categorical value
                # we need to check that (a) the column expects this categorical value and (b) this categorical has an associated CC
                if column_name in self.mapping_column_to_categorical_value and value in self.mapping_column_to_categorical_value[column_name]:
                    return_value = self.mapping_column_to_categorical_value[column_name][value]
                else:
                    # no categorical value for that value, we return the normalized value
                    self.quality_stats.add_unknown_categorical_value(column_name=column_name, categorical_value=value)
                    return_value = value
            elif etl_type == DataTypes.API:
                # there is no pre-defined list of the possible values, instead we create a CC based on the cell value,
                # which is an ontology resource
                split_value = value.split(":")  # we consider the ontology resource is of the form <ontology name>:<code>
                ontology_name = Ontologies.normalize_name(ontology_name=split_value[0])
                ontology_code = split_value[1]  # the code will be later normalized during the CC construction
                if value not in self.mapping_apivalue_to_onto_resource:
                    onto_resource = OntologyResource(
                        system=Ontologies.get_enum_from_name(ontology_name=ontology_name), code=ontology_code, label=None,
                        quality_stats=self.quality_stats)
                    self.mapping_apivalue_to_onto_resource[value] = onto_resource
                    return_value = onto_resource
                else:
                    return_value = self.mapping_apivalue_to_onto_resource[value]
            elif etl_type == DataTypes.DATETIME or etl_type == DataTypes.DATE:
                return_value = cast_str_to_datetime(str_value=value)
            elif etl_type == DataTypes.BOOLEAN:
                value = "1" if value == "1.0" else "0" if value == "0.0" else value
                return_value = cast_str_to_boolean(str_value=value)
                if return_value is None:
                    self.quality_stats.add_unknown_boolean_value(column_name=column_name, boolean_value=value)
                    return_value = value
            elif etl_type == DataTypes.INTEGER or etl_type == DataTypes.FLOAT:
                if column_name == self.execution.patient_id_column_name:
                    # do not cast int-like string identifiers as integers because this may add too much normalization
                    return_value = str(value)
                else:
                    # this is really a numeric value that we want to cast
                    m = re.search(PATTERN_VALUE_UNIT, value)
                    if m is not None:
                        # there is a unit in the data value
                        # m.group(0) is the text itself, m.group(1) is the int/float value, m.group(2) is the unit
                        the_value = m.group(1)
                        unit = m.group(2)
                        if unit == expected_unit:
                            if etl_type == DataTypes.INTEGER:
                                return_value = cast_str_to_int(str_value=the_value)
                            elif etl_type == DataTypes.FLOAT:
                                return_value = cast_str_to_float(str_value=the_value)
                        else:
                            # the feature unit does not correspond, we return the normalized (string) value
                            self.quality_stats.add_numerical_value_with_unmatched_unit(column_name=column_name,
                                                                                            expected_unit=expected_unit,
                                                                                            current_unit=unit,
                                                                                            value=value)
                            return_value = value
                    else:
                        # this value does not contain a unit or is not of the form "value unit"
                        # thus, we cast it depending on the ETL type
                        if etl_type == DataTypes.INTEGER:
                            return_value = cast_str_to_int(str_value=value)
                        elif etl_type == DataTypes.FLOAT:
                            return_value = cast_str_to_float(str_value=value)
                        else:
                            return_value = value
            else:
                # Unhandled ETL type: this cannot happen because all ETL types have been checked during the Extract step
                return_value = value

            # in case, casting the value returned None, we set the normalized value back
            # otherwise, we keep the cast value
            return_value = value if return_value is None else return_value
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
        etl_type = self.mapping_column_to_type[column_name] if column_name in self.mapping_column_to_type else DataTypes.STRING
        visibility = self.mapping_column_to_visibility[column_name] if column_name in self.mapping_column_to_visibility else Visibility.PRIVATE
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
