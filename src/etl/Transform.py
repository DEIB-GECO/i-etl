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
from etl.Task import Task
from profiles.DiagnosisFeature import DiagnosisFeature
from profiles.DiagnosisRecord import DiagnosisRecord
from profiles.Hospital import Hospital
from profiles.LaboratoryFeature import LaboratoryFeature
from profiles.LaboratoryRecord import LaboratoryRecord
from profiles.Patient import Patient
from profiles.Sample import Sample
from utils.Counter import Counter
from constants.defaults import BATCH_SIZE, PATTERN_VALUE_DIMENSION
from constants.idColumns import NO_ID, ID_COLUMNS
from utils.setup_logger import log
from utils.utils import is_not_nan, write_in_file, normalize_column_value, cast_str_to_datetime, \
    cast_str_to_float, cast_str_to_int, cast_str_to_boolean


class Transform(Task):

    def __init__(self, database: Database, execution: Execution, data: DataFrame, metadata: DataFrame,
                 mapping_categorical_value_to_cc: dict, mapping_column_to_categorical_value: dict,
                 mapping_column_to_dimension: dict, patient_ids_mapping: dict,
                 diagnosis_classification: dict, mapping_diagnosis_to_cc: dict):
        super().__init__(database, execution)
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
        # to know how to map "free text" diagnosis values and their classification (healthy/disease)
        self.diagnosis_classification = diagnosis_classification  # it may be None if this is not BUZZI
        self.mapping_diagnosis_to_cc = mapping_diagnosis_to_cc  # it may be None if this is not BUZZI

        # to record objects that will be further inserted in the database
        # features
        self.laboratory_features = []
        self.diagnosis_features = []
        self.medicine_features = []
        self.imaging_features = []
        self.genomic_features = []
        # records
        self.laboratory_records = []
        self.diagnosis_records = []
        self.medicine_records = []
        self.imaging_records = []
        self.genomic_records = []
        # other entities
        self.hospitals = []
        self.patients = []
        self.samples = []

        # to keep track off what is in the CSV and which objects (their identifiers) have been created out of it
        # this will also allow us to get resource identifiers of the referred resources
        self.mapping_hospital_to_hospital_id = {}  # map the hospital names to their IDs
        self.mapping_column_to_labfeat_id = {}  # map the column names to their LaboratoryFeature IDs
        self.mapping_column_to_diagfeat_id = {}  # map the disease names to their Diagnosis IDs

    def run(self) -> None:
        self.set_resource_counter_id()
        self.create_hospital()
        log.info(f"Hospital count: {self.database.count_documents(table_name=TableNames.HOSPITAL, filter_dict={})}")
        self.create_patients()
        # load some data from the database to compute references
        self.mapping_hospital_to_hospital_id = self.database.retrieve_identifiers(table_name=TableNames.HOSPITAL, projection="name")
        log.debug(f"{self.mapping_hospital_to_hospital_id}")

        if self.execution.current_file_type == FileTypes.LABORATORY:
            self.create_laboratory_features()
            self.create_samples()
            self.create_laboratory_records()
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
            self.create_genomic_features()
            self.create_genomic_records()
        else:
            raise TypeError(f"The current file type ({self.execution.current_file_type} is unknown. It should be laboratory, diagnosis, medicine, imaging or genomic.")

    ##############################################################
    # FEATURES
    ##############################################################

    def create_features(self, table_name: str, db_existing_features: list[str]) -> None:
        log.info(f"create Feature instances of type {table_name} in memory")
        count = 1
        for row_index, row in self.metadata.iterrows():
            column_name = row[MetadataColumns.COLUMN_NAME]
            # columns to remove have already been removed in the Extract part from the metadata
            # here, we need to ensure that we create Features only for still-existing columns and not for sample (that are managed aside) nor for ID column
            if column_name in self.metadata[MetadataColumns.COLUMN_NAME].values and column_name not in SampleColumns.values() and column_name != ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]:
                if column_name not in db_existing_features:
                    # we create a new Laboratory Feature from scratch
                    cc = self.create_codeable_concept_from_row(column_name=column_name)
                    if cc is not None:
                        # some columns have no attributed ontology code
                        # we still add the codeable_concept as a LabFeature
                        # but, it will have only the text (such as "BIS", or "TooYoung") and no associated codings
                        if table_name == TableNames.LABORATORY_FEATURE:
                            category = self.get_lab_feature_category(column_name=column_name)
                            data_type = row[MetadataColumns.ETL_TYPE]  # this has been normalized while loading + we take ETL_type, not var_type, to get the narrowest type (in which we cast values)
                            dimension = self.mapping_column_to_dimension[column_name]
                            # for categorical values, we first need to take the list of (normalized) values that are available for the current column, and then take their CC
                            categorical_values = None
                            if column_name in self.mapping_column_to_categorical_value:
                                normalized_categorical_values = self.mapping_column_to_categorical_value[column_name]
                                categorical_values = []
                                for normalized_categorical_value in normalized_categorical_values:
                                    categorical_values.append(self.mapping_categorical_value_to_cc[normalized_categorical_value])
                            new_lab_feature = LaboratoryFeature(id_value=NO_ID, code=cc, category=category,
                                                                permitted_datatype=data_type, dimension=dimension,
                                                                counter=self.counter, hospital_name=self.execution.hospital_name,
                                                                categorical_values=categorical_values)
                            log.info(f"adding a new LabFeature instance about {cc.text}: {new_lab_feature}")
                            self.laboratory_features.append(new_lab_feature)
                            if len(self.laboratory_features) >= BATCH_SIZE:
                                write_in_file(resource_list=self.laboratory_features, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_FEATURE, count=count)
                                self.laboratory_features = []
                                count = count + 1
                        elif table_name == TableNames.DIAGNOSIS_FEATURE:
                            data_type = row[MetadataColumns.ETL_TYPE]  # this has been normalized while loading + we take ETL_type, not var_type, to get the narrowest type (in which we cast values)
                            dimension = None  # no dimension for Diagnosis data because we only associate patient to their diagnosis (a disease name)
                            log.debug(f"for column {column_name}, dimension is {dimension}")
                            new_diag_feature = DiagnosisFeature(id_value=NO_ID, code=cc,
                                                                permitted_datatype=data_type,
                                                                dimension=dimension, counter=self.counter,
                                                                hospital_name=self.execution.hospital_name,
                                                                categorical_values=None)
                            log.info(f"adding a new DiagFeature instance about {cc.text}: {new_diag_feature}")
                            self.diagnosis_features.append(new_diag_feature)
                            if len(self.diagnosis_features) >= BATCH_SIZE:
                                write_in_file(resource_list=self.diagnosis_features,
                                              current_working_dir=self.execution.working_dir_current,
                                              table_name=TableNames.DIAGNOSIS_FEATURE, count=count)
                                self.diagnosis_features = []
                                count = count + 1
                        else:
                            raise NotImplementedError("To be implemented")
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
        self.write_remaining_instances_to_database(resources=self.laboratory_features, table_name=TableNames.LABORATORY_FEATURE, count=count, unique_variables=["code"])
        self.write_remaining_instances_to_database(resources=self.diagnosis_features, table_name=TableNames.DIAGNOSIS_FEATURE, count=count, unique_variables=["code"])

    def create_laboratory_features(self) -> None:
        log.info(f"create Lab. Feat. instances in memory")

        # 1. first, we retrieve the existing LabFeature instances to not recreate new CodeableConcepts for them
        # this will avoid to send again API queries to re-build already-built CodeableConcept
        result = self.database.find_operation(table_name=TableNames.LABORATORY_FEATURE, filter_dict={}, projection={"code.text": 1})
        db_lab_feature_names = []
        for res in result:
            db_lab_feature_names.append(res["code"]["text"])

        # 2., then we can create non-existing LabFeature instances
        self.create_features(table_name=TableNames.LABORATORY_FEATURE, db_existing_features=db_lab_feature_names)

    def create_diagnosis_features(self):
        log.info(f"create Diag. Feat. instances in memory")

        # 1. first, we retrieve the existing DiagFeature instances to not recreate new CodeableConcepts for them
        # this will avoid to send again API queries to re-build already-built CodeableConcept
        # TODO NELLY: select distinct based on coding system+code
        # TODO Nelly: Do the same for laboratory records, etc...: get their codeableConcept in-memory first? or maybe compute the display only if we insert it in the db?
        result = self.database.find_operation(table_name=TableNames.DIAGNOSIS_FEATURE, filter_dict={}, projection={"code.text": 1})
        db_diag_feature_names = []
        for res in result:
            db_diag_feature_names.append(res["code"]["text"])
        self.create_features(table_name=TableNames.DIAGNOSIS_FEATURE, db_existing_features=db_diag_feature_names)

    def create_medicine_features(self):
        # TODO Nelly: implement this
        pass

    def create_imaging_features(self):
        # TODO Nelly: implement this
        pass

    def create_genomic_features(self):
        # TODO Nelly: implement this
        pass

    ##############################################################
    # RECORDS
    ##############################################################

    def create_laboratory_records(self) -> None:
        log.info(f"create Lab. Rec. instances in memory")

        # a. load some data from the database to compute references
        self.mapping_hospital_to_hospital_id = self.database.retrieve_identifiers(table_name=TableNames.HOSPITAL, projection="name")
        log.debug(f"{self.mapping_hospital_to_hospital_id}")

        self.mapping_column_to_labfeat_id = self.database.retrieve_identifiers(table_name=TableNames.LABORATORY_FEATURE, projection="code.text")
        log.debug(f"{self.mapping_column_to_labfeat_id}")

        # b. Create LabRecord instance, and write them in temporary (JSON) files
        count = 1
        log.info(self.mapping_column_to_labfeat_id)
        for index, row in self.data.iterrows():
            # create LabRecord instances by associating observations to patients (and possibly the sample)
            for column_name, value in row.items():
                # log.debug(f"for row {index} (type: {type(index)}) and column {column_name} (type: {type(column_name)}), value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that LabFeature, no need to create a LabRecord instance
                    # log.error(f"skipping value {value} because it is None, or empty or nan")
                    pass
                else:
                    if column_name in self.mapping_column_to_labfeat_id:
                        # we know a code for this column, so we can register the value of that LabFeature
                        lab_feature_id = Identifier(self.mapping_column_to_labfeat_id[column_name])
                        hospital_id = Identifier(self.mapping_hospital_to_hospital_id[self.execution.hospital_name])
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                        # we cannot use PatientAnonymizedIdentifier otherwise we would concat a set hospital name to the anonymized patient id
                        # patient_id = PatientAnonymizedIdentifier(id_value=self.patient_ids_mapping[row[id_column_for_patients]], hospital_name=self.execution.hospital_name)
                        patient_id = Identifier(value=self.patient_ids_mapping[row[id_column_for_patients]])
                        id_column_for_samples = ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE] if TableNames.SAMPLE in ID_COLUMNS[self.execution.hospital_name] else ""
                        sample_id = None
                        if id_column_for_samples != "":
                            sample_id = Identifier(value=row[id_column_for_samples])
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        new_laboratory_record = LaboratoryRecord(id_value=NO_ID, feature_id=lab_feature_id,
                                                                 patient_id=patient_id, hospital_id=hospital_id,
                                                                 sample_id=sample_id, value=fairified_value,
                                                                 counter=self.counter, hospital_name=self.execution.hospital_name)
                        self.laboratory_records.append(new_laboratory_record)
                        if len(self.laboratory_records) >= BATCH_SIZE:
                            write_in_file(resource_list=self.laboratory_records, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_RECORD, count=count)
                            # no need to load LabRecords instances because they are never referenced
                            self.laboratory_records = []
                            count = count + 1
                    else:
                        # this represents the case when a column has not been converted to a LabFeature resource
                        # this may happen for ID column for instance
                        log.error(f"Skipping column {column_name} for row {index}")
                        pass

        write_in_file(resource_list=self.laboratory_records, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_RECORD, count=count)

    def create_diagnosis_records(self):
        log.info(f"create Diag. Rec. instances in memory")

        self.mapping_column_to_diagfeat_id = self.database.retrieve_identifiers(table_name=TableNames.DIAGNOSIS_FEATURE, projection="code.text")
        log.debug(f"{self.mapping_column_to_diagfeat_id}")

        # b. Create DiagRecord instance, and write them in temporary (JSON) files
        count = 1
        for index, row in self.data.iterrows():
            # create DiagRecord instances by associating diagnoses to patients
            for column_name, value in row.items():
                # log.debug(f"for row {index} and column {column_name}, value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that LabFeature, no need to create a LabRecord instance
                    pass
                else:
                    if column_name in self.mapping_column_to_diagfeat_id:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that LabFeature
                        diag_feature_id = Identifier(value=self.mapping_column_to_diagfeat_id[column_name])
                        hospital_id = Identifier(self.mapping_hospital_to_hospital_id[self.execution.hospital_name])
                        # for patient instances, no need to go through a mapping because they have an ID assigned by the hospital
                        # TODO NELLY: if Buzzi decides to remove patient ids, we will have to number them with a Counter
                        #  and to create mappings (as for Hospital and Feature resources)
                        id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                        # we cannot use PatientAnonymizedIdentifier otherwise we would concat a set hospital name to the anonymized patient id
                        # patient_id = PatientAnonymizedIdentifier(id_value=row[id_column_for_patients], hospital_name=self.execution.hospital_name)
                        patient_id = Identifier(value=self.patient_ids_mapping[row[id_column_for_patients]])
                        # log.debug(f"patient_id = {patient_id.to_json()} of type {type(patient_id)}")
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        if fairified_value is None:
                            # the patient has genetic mutation but he is not ill
                            # thus, we do not record it in database
                            pass
                        else:
                            new_diag_record = DiagnosisRecord(id_value=NO_ID, feature_id=diag_feature_id,
                                                              patient_id=patient_id, hospital_id=hospital_id,
                                                              value=fairified_value, counter=self.counter,
                                                              hospital_name=self.execution.hospital_name)
                            self.diagnosis_records.append(new_diag_record)
                            if len(self.diagnosis_records) >= BATCH_SIZE:
                                write_in_file(resource_list=self.diagnosis_records,
                                              current_working_dir=self.execution.working_dir_current,
                                              table_name=TableNames.DIAGNOSIS_RECORD, count=count)
                                # no need to load LabRecords instances because they are never referenced
                                self.diagnosis_records = []
                                count = count + 1
                    else:
                        # this should never happen
                        # this represents the case when a column has not been converted to a LabFeature resource
                        pass

        write_in_file(resource_list=self.diagnosis_records, current_working_dir=self.execution.working_dir_current,
                      table_name=TableNames.DIAGNOSIS_RECORD, count=count)

    def create_medicine_records(self):
        # TODO Nelly: implement this
        pass

    def create_imaging_records(self):
        # TODO Nelly: implement this
        pass

    def create_genomic_records(self):
        # TODO Nelly: implement this
        pass

    ##############################################################
    # OTHER ENTITIES
    ##############################################################

    def create_samples(self) -> None:
        if TableNames.SAMPLE in ID_COLUMNS[self.execution.hospital_name] and ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE] in self.data.columns:
            # this is a dataset with samples
            log.info(f"create Sample instances in memory")
            created_sample_barcodes = set()
            count = 1
            for index, row in self.data.iterrows():
                sample_barcode = row[SampleColumns.SAMPLE_BAR_CODE]
                if sample_barcode not in created_sample_barcodes:
                    sampling = row[SampleColumns.SAMPLING] if SampleColumns.SAMPLING in row else None
                    sample_quality = row[SampleColumns.SAMPLE_QUALITY] if SampleColumns.SAMPLE_QUALITY in row else None
                    time_collected = cast_str_to_datetime(str_value=row[SampleColumns.SAMPLE_COLLECTED]) if SampleColumns.SAMPLE_COLLECTED in row else None
                    time_received = cast_str_to_datetime(str_value=row[SampleColumns.SAMPLE_RECEIVED]) if SampleColumns.SAMPLE_RECEIVED in row else None
                    too_young = cast_str_to_boolean(str_value=row[SampleColumns.SAMPLE_TOO_YOUNG]) if SampleColumns.SAMPLE_TOO_YOUNG in row else None
                    bis = cast_str_to_boolean(str_value=row[SampleColumns.SAMPLE_BIS]) if SampleColumns.SAMPLE_BIS in row else None
                    new_sample = Sample(sample_barcode, sampling=sampling, quality=sample_quality,
                                        time_collected=time_collected, time_received=time_received,
                                        too_young=too_young, bis=bis, counter=self.counter, hospital_name=self.execution.hospital_name)
                    created_sample_barcodes.add(sample_barcode)
                    self.samples.append(new_sample)
                    if len(self.samples) >= BATCH_SIZE:
                        write_in_file(resource_list=self.samples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.SAMPLE, count=count)
                        self.samples = []
                        count = count + 1
                        # no need to load Sample instances because they are referenced using their ID,
                        # which was provided by the hospital (thus is known by the dataset)
            write_in_file(resource_list=self.samples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.SAMPLE, count=count)
            log.debug(f"Created distinct {len(created_sample_barcodes)} samples in total.")
        else:
            log.debug("This hospital should not have samples. Skipping it.")

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
            if is_not_nan(row[MetadataColumns.FIRST_ONTOLOGY_NAME]):
                coding = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(row[MetadataColumns.FIRST_ONTOLOGY_NAME]), full_code=row[MetadataColumns.FIRST_ONTOLOGY_CODE]),
                                display=None)
                cc.add_coding(one_coding=coding)
            if is_not_nan(row[MetadataColumns.SEC_ONTOLOGY_NAME]):
                coding = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(row[MetadataColumns.SEC_ONTOLOGY_NAME]), full_code=row[MetadataColumns.SEC_ONTOLOGY_CODE]),
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
        the_normalized_value = normalize_column_value(column_value=value)  # we compute only once the cast value and return it whenever we cannot FAIRify deeply the value
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
                # log.info("return the cast value")
                return_value = the_normalized_value  # no categorical value for that value, trying at least to normalize it a bit
        elif etl_type == DataTypes.DATETIME or etl_type == DataTypes.DATE:
            # we first process the date and then maybe hide the day if asked
            cast_date = cast_str_to_datetime(str_value=value)
            if self.execution.expose_complete_dates:
                # hide the date
                cast_date_without_day = cast_date  # TODO Nelly: finish this
                return_value = cast_date_without_day
            else:
                return_value = cast_date
        elif etl_type == DataTypes.BOOLEAN:
            # boolean values may appear as (a) CC (si/no or 0/1), or (b) 0/1 or 0.0/1.0 (1.0/0.0 has to be converted to 1/0)
            value = "1" if value == "1.0" else "0" if value == "0.0" else value
            # and that same value is also available in the mapping to cc
            if column_name in self.mapping_column_to_categorical_value and value in self.mapping_column_to_categorical_value[column_name] and value in self.mapping_categorical_value_to_cc:
                cc = self.mapping_categorical_value_to_cc[value]
                # this should rather be a boolean, let's cast it as boolean, instead of using Yes/No SNOMED_CT codes
                if cc == TrueFalse.get_true():
                    # log.info("return True")
                    return_value = True
                elif cc == TrueFalse.get_false():
                    # log.info("return False")
                    return_value = False
                else:
                    # log.info("return the cast value")
                    return_value = the_normalized_value
            else:
                # log.info("return the cast value")
                return_value = cast_str_to_boolean(str_value=value)  # no coded value for that value, trying to cast it as boolean
        elif etl_type == DataTypes.INTEGER or etl_type == DataTypes.FLOAT:
            # if the dimension is not the one of the feature, we simply leave the cell value as a string
            # otherwise, convert the numeric value
            m = re.search(PATTERN_VALUE_DIMENSION, value)
            if m is not None:
                # m.group(0) is the text itself, m.group(1) is the int/float value, m.group(2) is the dimension
                the_value = m.group(1)
                unit = m.group(2)
                if unit == expected_unit:
                    if etl_type == DataTypes.INTEGER:
                        # log.info("return int")
                        return_value = cast_str_to_int(str_value=the_value)
                    elif etl_type == DataTypes.FLOAT:
                        # log.info("return float")
                        return_value = cast_str_to_float(str_value=the_value)
                else:
                    # the feature dimension does not correspond, we return the normalized value
                    # log.info("return the value")
                    return_value = the_normalized_value
            else:
                # this value does not contain a dimension or is not of the form "value dimension"
                # thus, we cast it depending on the ETL type
                if etl_type == DataTypes.INTEGER:
                    # log.info("return int")
                    return_value = cast_str_to_int(str_value=value)
                elif etl_type == DataTypes.FLOAT:
                    # log.info("return float")
                    return_value = cast_str_to_float(str_value=value)
        elif etl_type == DataTypes.REGEX:
            # this corresponds to a diagnosis name, for which we need to find the corresponding ORPHANET code
            if value in self.diagnosis_classification:
                # the "free text" diagnosis value has a corresponding standard (textual) diagnosis name
                if "classification" in self.diagnosis_classification[value]:
                    if self.diagnosis_classification[value]["classification"] == normalize_column_value("Disease"):
                        if "standard_name" in self.diagnosis_classification[value]:
                            diagnosis_name = self.diagnosis_classification[value]["standard_name"]
                            if diagnosis_name in self.mapping_diagnosis_to_cc:
                                # the standard diagnosis name has corresponding codes (ORPHANET and OMIM)
                                the_cc = self.mapping_diagnosis_to_cc[diagnosis_name]
                                log.info("return cc")
                                return_value = the_cc
                            else:
                                log.error(f"For '{diagnosis_name}', no existing CC has been found. Will return '{the_normalized_value}'")
                                log.info("return the cast value")
                                return_value = the_normalized_value
                        else:
                            log.error(f"'{value}' has no associated standard name. Will return '{the_normalized_value}'")
                            log.info("return the cast value")
                            return_value = the_normalized_value
                    else:
                        log.error("This disease is not a real disease (because such patient do not show abnormal conditions). Will return None")
                        log.info("return None")
                        return_value = None
                else:
                    log.error(f"No classification for '{value}'. Will return '{the_normalized_value}'")
                    log.info("return the cast value")
                    return_value = the_normalized_value
            else:
                log.error(f"For '{value}', no diagnosis name has been found. Will return '{the_normalized_value}'")
                log.info("return the cast value")
                return_value = the_normalized_value
        else:
            log.error(f"Unhandled ETL type '{etl_type}'")

        # in case, casting the value returned None, we set the original value back
        # otherwise, we keep the cast value
        return_value = value if return_value is None else return_value

        # we use type(..).__name__ to get the class name, e.g., "str" or "bool", instead of "<class 'float'>"
        log.info(f"Column '{column_name}': fairify {type(value).__name__} value '{value}' (unit: {expected_unit}) into {type(return_value).__name__}: {return_value}")
        return return_value
