import copy
import re
from datetime import datetime
from typing import Any

import numpy as np
from pandas import DataFrame

from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.Identifier import Identifier
from datatypes.Reference import Reference
from enums.ColumnsToIgnore import ColumnsToIgnore
from enums.DataTypes import DataTypes
from enums.FileTypes import FileTypes
from enums.PhenotypicColumns import PhenotypicColumns
from enums.SampleColumns import SampleColumns
from enums.TrueFalse import TrueFalse
from profiles.DiagnosisFeature import DiagnosisFeature
from profiles.DiagnosisRecord import DiagnosisRecord
from profiles.LaboratoryFeature import LaboratoryFeature
from profiles.LaboratoryRecord import LaboratoryRecord
from profiles.Hospital import Hospital
from profiles.Patient import Patient
from profiles.Sample import Sample
from utils.Counter import Counter
from enums.LabFeatureCategories import LabFeatureCategories
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from utils.constants import NO_ID, BATCH_SIZE, ID_COLUMNS, PATTERN_VALUE_DIMENSION
from utils.setup_logger import log
from utils.utils import is_not_nan, cast_value, write_in_file


class Transform:

    def __init__(self, database: Database, execution: Execution, data: DataFrame, metadata: DataFrame, mapped_values: dict, column_to_dimension: dict):
        self.database = database
        self.execution = execution
        self.counter = Counter()

        # get data, metadata and the mapped values computed in the Extract step
        self.data = data
        self.metadata = metadata
        self.mapped_values = mapped_values
        self.column_to_dimension = column_to_dimension

        # to record objects that will be further inserted in the database
        self.hospitals = []
        self.patients = []
        self.laboratory_features = []
        self.laboratory_records = []
        self.samples = []
        self.diagnosis_features = []
        self.diagnosis_records = []
        self.medicine_features = []
        self.medicine_records = []
        self.imaging_features = []
        self.medicine_records = []
        self.genomic_features = []
        self.genomic_records = []

        # to keep track off what is in the CSV and which objects (their identifiers) have been created out of it
        # this will also allow us to get resource identifiers of the referred resources
        self.mapping_hospital_to_hospital_id = {}  # map the hospital names to their IDs
        self.mapping_column_to_labfeat_id = {}  # map the column names to their LaboratoryFeature IDs
        self.mapping_column_to_diagfeat_id = {}  # map the disease names to their Diagnosis IDs

    def run(self) -> None:
        self.set_resource_counter_id()
        self.create_hospital(hospital_name=self.execution.hospital_name)
        log.info(f"Hospital count: {self.database.count_documents(table_name=TableNames.HOSPITAL, filter_dict={})}")
        self.create_patients()
        # load some data from the database to compute references
        self.mapping_hospital_to_hospital_id = self.database.retrieve_identifiers(table_name=TableNames.HOSPITAL, projection="name")
        log.debug(f"{self.mapping_hospital_to_hospital_id}")

        if self.execution.current_file_type == FileTypes.LABORATORY:
            self.create_laboratory_features()
            log.info(f"LabFeature count: {self.database.count_documents(table_name=TableNames.LABORATORY_FEATURE, filter_dict={})}")
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
            raise TypeError(
                f"The type of the current file is unknown. It should be laboratory, diagnosis, medicine, imaging or genomic. It is of type: {self.execution.current_file_type}")

    def set_resource_counter_id(self) -> None:
        self.counter.set_with_database(database=self.database)

    def create_hospital(self, hospital_name: str) -> None:
        log.info(f"create hospital instance in memory")
        new_hospital = Hospital(id_value=NO_ID, name=hospital_name, counter=self.counter)
        self.hospitals.append(new_hospital)
        write_in_file(resource_list=self.hospitals, current_working_dir=self.execution.working_dir_current, table_name=TableNames.HOSPITAL, count=1)
        self.database.load_json_in_table(table_name=TableNames.HOSPITAL, unique_variables=["name"])

    def create_laboratory_features(self) -> None:
        log.info(f"create Lab. Feat. instances in memory")
        count = 1
        index_for_column_name = self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)
        log.debug(f"The index of {MetadataColumns.COLUMN_NAME} is {index_for_column_name}")
        for row_index, row in self.metadata.iterrows():
            column_name = row[MetadataColumns.COLUMN_NAME]
            log.debug(f"Row about {column_name}")
            log.debug(ColumnsToIgnore.values())
            if column_name not in SampleColumns.values() and column_name not in ColumnsToIgnore.values():
                cc = self.create_codeable_concept_from_row(column_name=column_name)
                log.debug(cc)
                if cc is not None:
                    # some columns have no attributed ontology code
                    # we still add the codeable_concept as a LabFeature
                    # but, it will have only the text (such as "BIS", or "TooYoung") and no associated codings
                    category = Transform.get_lab_feature_category(column_name=column_name)
                    data_type = row[MetadataColumns.ETL_TYPE]  # this has been normalized while loading + wetake ETL_type, not var_type, to get the narrowest type (in which we cast values)
                    dimension = self.column_to_dimension[column_name]
                    log.debug(f"for column {column_name}, dimension is {dimension}")
                    new_lab_feature = LaboratoryFeature(id_value=NO_ID, code=cc, category=category, permitted_datatype=data_type, dimension=dimension, counter=self.counter)
                    log.info(f"adding a new LabFeature instance about {cc.text}: {new_lab_feature}")
                    self.laboratory_features.append(new_lab_feature)
                    if len(self.laboratory_features) >= BATCH_SIZE:
                        write_in_file(resource_list=self.laboratory_features, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_FEATURE, count=count)
                        self.laboratory_features = []
                        count = count + 1
                else:
                    # the column was not present in the data, or it was present several times
                    # this should never happen
                    pass
            else:
                log.debug(f"I am skipping column {column_name} because it has been marked as not being part of LabFeature instances.")
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        write_in_file(resource_list=self.laboratory_features, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_FEATURE, count=count)
        self.database.load_json_in_table(table_name=TableNames.LABORATORY_FEATURE, unique_variables=["code"])

    def create_samples(self) -> None:
        if ID_COLUMNS[HospitalNames.IT_BUZZI_UC1][TableNames.SAMPLE] in self.data.columns:
            # this is a dataset with samples
            log.info(f"create Sample instances in memory")
            created_sample_barcodes = set()
            count = 1
            for index, row in self.data.iterrows():
                sample_barcode = row[SampleColumns.SAMPLE_BAR_CODE]
                if sample_barcode not in created_sample_barcodes:
                    sampling = row[SampleColumns.SAMPLING] if SampleColumns.SAMPLING in row else None
                    sample_quality = row[SampleColumns.SAMPLE_QUALITY] if SampleColumns.SAMPLE_QUALITY in row else None
                    time_collected = cast_value(value=row[SampleColumns.SAMPLE_COLLECTED]) if SampleColumns.SAMPLE_COLLECTED in row else None
                    time_received = cast_value(value=row[SampleColumns.SAMPLE_RECEIVED]) if SampleColumns.SAMPLE_RECEIVED in row else None
                    too_young = cast_value(value=row[SampleColumns.SAMPLE_TOO_YOUNG]) if SampleColumns.SAMPLE_TOO_YOUNG in row else None
                    bis = cast_value(value=row[SampleColumns.SAMPLE_BIS]) if SampleColumns.SAMPLE_BIS in row else None
                    new_sample = Sample(sample_barcode, sampling=sampling, quality=sample_quality,
                                        time_collected=time_collected, time_received=time_received,
                                        too_young=too_young, bis=bis, counter=self.counter)
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

    def create_laboratory_records(self) -> None:
        log.info(f"create Lab. Rec. instances in memory")

        # a. load some data from the database to compute references
        self.mapping_hospital_to_hospital_id = self.database.retrieve_identifiers(table_name=TableNames.HOSPITAL, projection="name")
        log.debug(f"{self.mapping_hospital_to_hospital_id}")

        self.mapping_column_to_labfeat_id = self.database.retrieve_identifiers(table_name=TableNames.LABORATORY_FEATURE, projection="code.text")
        log.debug(f"{self.mapping_column_to_labfeat_id}")

        # b. Create LabRecord instance, and write them in temporary (JSON) files
        count = 1
        for index, row in self.data.iterrows():
            # create LabRecord instances by associating observations to patients (and possibly the sample)
            for column_name, value in row.items():
                log.debug(f"for row {index} (type: {type(index)}) and column {column_name} (type: {type(column_name)}), value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that LabFeature, no need to create a LabRecord instance
                    pass
                else:
                    if column_name in self.mapping_column_to_labfeat_id:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that LabFeature
                        lab_feature_id = self.mapping_column_to_labfeat_id[column_name]
                        lab_feature_ref = Reference(resource_identifier=lab_feature_id, resource_type=TableNames.LABORATORY_FEATURE)
                        hospital_id = self.mapping_hospital_to_hospital_id[self.execution.hospital_name]
                        hospital_ref = Reference(resource_identifier=hospital_id, resource_type=TableNames.HOSPITAL)
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        # TODO NELLY: if Buzzi decides to remove patient ids, we will have to number them with a Counter
                        #  and to create mappings (as for Hospital and LabFeature resources)
                        id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                        patient_id = Identifier(id_value=row[id_column_for_patients], resource_type=TableNames.PATIENT)
                        log.debug(f"patient_id = {patient_id.to_json()} of type {type(patient_id)}")
                        patient_ref = Reference(resource_identifier=patient_id.value, resource_type=TableNames.PATIENT)
                        log.debug(f"patient_ref = {patient_ref.to_json()} of type {type(patient_ref)}")
                        id_column_for_samples = ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE] if TableNames.SAMPLE in ID_COLUMNS[self.execution.hospital_name] else ""
                        sample_ref = None
                        if id_column_for_samples != "":
                            sample_id = Identifier(id_value=row[id_column_for_samples], resource_type=TableNames.SAMPLE)
                            sample_ref = Reference(resource_identifier=sample_id.value, resource_type=TableNames.SAMPLE)
                        # TODO Nelly: we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        log.info(column_name)
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        new_laboratory_record = LaboratoryRecord(id_value=NO_ID, feature_ref=lab_feature_ref,
                                                                 patient_ref=patient_ref, hospital_ref=hospital_ref,
                                                                 sample_ref=sample_ref, value=fairified_value,
                                                                 counter=self.counter)
                        self.laboratory_records.append(new_laboratory_record)
                        if len(self.laboratory_records) >= BATCH_SIZE:
                            write_in_file(resource_list=self.laboratory_records, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_RECORD, count=count)
                            # no need to load LabRecords instances because they are never referenced
                            self.laboratory_records = []
                            count = count + 1
                    else:
                        # this should never happen
                        # this represents the case when a column has not been converted to a LabFeature resource
                        pass

        write_in_file(resource_list=self.laboratory_records, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_RECORD, count=count)

    def create_patients(self) -> None:
        log.info(f"create patient instances in memory")
        created_patient_ids = set()
        count = 1
        for index, row in self.data.iterrows():
            log.debug(row["id"])
            patient_id = row[ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]]
            log.info(f"patient id in data: {patient_id} of type {type(patient_id)}")
            if patient_id not in created_patient_ids:
                # the patient does not exist yet, we will create it
                new_patient = Patient(id_value=patient_id, counter=self.counter)
                created_patient_ids.add(patient_id)
                self.patients.append(new_patient)
                if len(self.patients) >= BATCH_SIZE:
                    write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current, table_name=TableNames.PATIENT, count=count)  # this will save the data if it has reached BATCH_SIZE
                    self.patients = []
                    count = count + 1
                    # no need to load Patient instances because they are referenced using their ID,
                    # which was provided by the hospital (thus is known by the dataset)
        write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current, table_name=TableNames.PATIENT, count=count)

    def create_diagnosis_features(self):
        log.info(f"create Diag. Feat. instances in memory")
        count = 1
        index_for_column_name = self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)
        log.debug(f"The index of {MetadataColumns.COLUMN_NAME} is {index_for_column_name}")
        for row_index, row in self.metadata.iterrows():
            column_name = row[MetadataColumns.COLUMN_NAME]
            log.debug(f"Row about {column_name}")
            log.debug(ColumnsToIgnore.values())
            if column_name not in SampleColumns.values() and column_name not in ColumnsToIgnore.values():
                cc = self.create_codeable_concept_from_row(column_name=column_name)
                log.debug(cc)
                if cc is not None:
                    # some columns have no attributed ontology code
                    # we still add the codeable_concept as a LabFeature
                    # but, it will have only the text (such as "BIS", or "TooYoung") and no associated codings
                    data_type = row[MetadataColumns.ETL_TYPE]  # this has been normalized while loading + we take ETL_type, not var_type, to get the narrowest type (in which we cast values)
                    dimension = None  # no dimension for Diagnosis data because we only associate patient to their diagnosis (a disease name)
                    log.debug(f"for column {column_name}, dimension is {dimension}")
                    new_diag_feature = DiagnosisFeature(id_value=NO_ID, code=cc, permitted_datatype=data_type,
                                                        dimension=dimension, counter=self.counter)
                    log.info(f"adding a new DiagFeature instance about {cc.text}: {new_diag_feature}")
                    self.diagnosis_features.append(new_diag_feature)
                    if len(self.diagnosis_features) >= BATCH_SIZE:
                        write_in_file(resource_list=self.diagnosis_features,
                                      current_working_dir=self.execution.working_dir_current,
                                      table_name=TableNames.DIAGNOSIS_FEATURE, count=count)
                        self.diagnosis_features = []
                        count = count + 1
                else:
                    # the column was not present in the data, or it was present several times
                    # this should never happen
                    pass
            else:
                log.debug(f"I am skipping column {column_name} because it has been marked as not being part of DiagFeature instances.")
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        write_in_file(resource_list=self.diagnosis_features, current_working_dir=self.execution.working_dir_current,
                      table_name=TableNames.DIAGNOSIS_FEATURE, count=count)
        self.database.load_json_in_table(table_name=TableNames.DIAGNOSIS_FEATURE, unique_variables=["code"])

    def create_diagnosis_records(self):
        log.info(f"create Diag. Rec. instances in memory")

        self.mapping_column_to_diagfeat_id = self.database.retrieve_identifiers(table_name=TableNames.DIAGNOSIS_FEATURE, projection="code.text")
        log.debug(f"{self.mapping_column_to_diagfeat_id}")

        # b. Create DiagRecord instance, and write them in temporary (JSON) files
        count = 1
        for index, row in self.data.iterrows():
            # create DiagRecord instances by associating diagnoses to patients
            for column_name, value in row.items():
                log.debug(f"for row {index} (type: {type(index)} and column {column_name} (type: {type(column_name)}), value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that LabFeature, no need to create a LabRecord instance
                    pass
                else:
                    if column_name in self.mapping_column_to_diagfeat_id:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that LabFeature
                        diag_feature_id = self.mapping_column_to_diagfeat_id[column_name]
                        diag_feature_ref = Reference(resource_identifier=diag_feature_id, resource_type=TableNames.DIAGNOSIS_FEATURE)
                        hospital_id = self.mapping_hospital_to_hospital_id[self.execution.hospital_name]
                        hospital_ref = Reference(resource_identifier=hospital_id, resource_type=TableNames.HOSPITAL)
                        # for patient instances, no need to go through a mapping because they have an ID assigned by the hospital
                        # TODO NELLY: if Buzzi decides to remove patient ids, we will have to number them with a Counter
                        #  and to create mappings (as for Hospital and Feature resources)
                        id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT]
                        patient_id = Identifier(id_value=row[id_column_for_patients], resource_type=TableNames.PATIENT)
                        log.debug(f"patient_id = {patient_id.to_json()} of type {type(patient_id)}")
                        patient_ref = Reference(resource_identifier=patient_id.value, resource_type=TableNames.PATIENT)
                        log.debug(f"patient_ref = {patient_ref.to_json()} of type {type(patient_ref)}")
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        new_diag_record = DiagnosisRecord(id_value=NO_ID, feature_ref=diag_feature_ref,
                                                             patient_ref=patient_ref, hospital_ref=hospital_ref,
                                                             value=fairified_value, counter=self.counter)
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

    def create_medicine_features(self):
        # TODO Nelly: implement this
        pass

    def create_medicine_records(self):
        # TODO Nelly: implement this
        pass

    def create_imaging_features(self):
        # TODO Nelly: implement this
        pass

    def create_imaging_records(self):
        # TODO Nelly: implement this
        pass

    def create_genomic_features(self):
        # TODO Nelly: implement this
        pass

    def create_genomic_records(self):
        # TODO Nelly: implement this
        pass

    # Transform UTILITY methods

    def create_codeable_concept_from_row(self, column_name: str) -> CodeableConcept | None:
        cc = CodeableConcept()
        rows = self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name]
        log.debug(rows)
        if len(rows) == 1:
            row = rows.iloc[0]
            if is_not_nan(row[MetadataColumns.FIRST_ONTOLOGY_SYSTEM]):
                coding = Coding(system=Ontologies.get_ontology_system(row[MetadataColumns.FIRST_ONTOLOGY_SYSTEM]),
                                code=row[MetadataColumns.FIRST_ONTOLOGY_CODE],
                                name=row[MetadataColumns.COLUMN_NAME],
                                description=row[MetadataColumns.SIGNIFICATION_EN])
                cc.add_coding(one_coding=coding)
            if is_not_nan(row[MetadataColumns.SEC_ONTOLOGY_SYSTEM]):
                coding = Coding(system=Ontologies.get_ontology_system(row[MetadataColumns.SEC_ONTOLOGY_SYSTEM]),
                                code=row[MetadataColumns.SEC_ONTOLOGY_CODE],
                                name=row[MetadataColumns.COLUMN_NAME],
                                description=row[MetadataColumns.SIGNIFICATION_EN])
                cc.add_coding(one_coding=coding)
            # NB July 18th 2024: for the text of a CC, this HAS TO be the column name ONLY (and not the column name and the description).
            # this is because we build a mapping between column names (variables) and their identifiers
            # if there is also the description in the text, then no column name would match the CC text.
            # NOPE   cc.text = LabFeature.get_display(column_name=row[MetadataColumns.COLUMN_NAME], column_description=row[MetadataColumns.SIGNIFICATION_EN])  # the column name + description
            cc.text = row[MetadataColumns.COLUMN_NAME]
            return cc
        elif len(rows) == 0:
            # log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            # log.warn("Found several times the column '%s' in the metadata", column_name)
            return None

    @classmethod
    def get_lab_feature_category(cls, column_name: str) -> CodeableConcept:
        if PhenotypicColumns.is_column_phenotypic(column_name=column_name):
            return LabFeatureCategories.get_phenotypic()
        else:
            return LabFeatureCategories.get_clinical()

    def fairify_value(self, column_name: str, value: Any) -> str | float | datetime | CodeableConcept:
        current_column_info = DataFrame(self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME] == column_name])
        etl_type = current_column_info[MetadataColumns.ETL_TYPE].iloc[0]  # we need to take the value itself, otherwise we end up with a series of 1 element (the ETL type)
        var_type = current_column_info[MetadataColumns.VAR_TYPE].iloc[0]
        if column_name in self.column_to_dimension:
            expected_unit = self.column_to_dimension[column_name]
        else:
            expected_unit = None  # in case no unit has been specified and no unit could be extracted form data
        log.debug(f"ETL type is {etl_type}, var type is {var_type}, expected unit is {expected_unit}")
        if (etl_type == DataTypes.CATEGORY or etl_type == DataTypes.BOOLEAN) and column_name in self.mapped_values:
            # we iterate over all the mappings of a given column
            for mapping in self.mapped_values[column_name]:
                # we get the value of the mapping, e.g., F, or M, or NA
                mapped_value = mapping['value']
                # if the sample value is equal to the mapping value, we have found a match,
                # and we will record the associated ontology term instead of the value
                if value == mapped_value:
                    # we create a CodeableConcept with each code added to the mapping, e.g., snomed_ct and loinc
                    # recall that a mapping is of the form: {'value': 'X', 'explanation': '...', 'snomed_ct': '123', 'loinc': '456' }
                    # and we add each ontology code to that CodeableConcept
                    cc = CodeableConcept()
                    for key, val in mapping.items():
                        # for any key value pair that is not about the value or the explanation
                        # (i.e., loinc and snomed_ct columns), we create a Coding, which we add to the CodeableConcept
                        # we need to do a loop because there may be several ontology terms for a single mapping
                        if key != 'value' and key != 'explanation':
                            system = Ontologies.get_ontology_system(ontology=key)
                            cc.add_coding(one_coding=Coding(system=system, code=val, name=value, description=mapping['explanation']))
                    # now that we have constructed a CC from the value, we check whether this corresponds to the categorical value of booleans
                    # i.e., True/False(/unknown), Sì/no, etc
                    if etl_type == DataTypes.BOOLEAN:
                        log.info(f"The value {value} should rather be a boolean. ")
                        # this should rather be a boolean, let's cast it as boolean, instead of using Yes/No SNOMED_CT codes
                        if cc == TrueFalse.get_true():
                            return True
                        elif cc == TrueFalse.get_false():
                            return False
                        else:
                            return np.nan
                    else:
                        return cc  # return the CC computed out of the corresponding mapping
            return cast_value(value=value)  # no coded value for that value, trying at least to normalize it a bit
        elif var_type == DataTypes.STRING and (etl_type == DataTypes.INTEGER or etl_type == DataTypes.FLOAT):
            # if the dimension is not the one of the feature, we simply leave the cell value as a string
            # otherwise, convert the numeric value
            log.debug(f"convert str value to int or float: {value}")
            m = re.search(PATTERN_VALUE_DIMENSION, value)
            log.debug(f"convert str value to int or float: {value}")
            if m is not None:
                # m.group(0) is the text itself, m.group(1) is the int/float value, m.group(2) is the dimension
                the_value = m.group(1)
                unit = m.group(2)
                log.debug(f"could convert str value to int or float: {value}. expected unit is {expected_unit}, unit is {unit}")
                if unit == expected_unit:
                    if etl_type == DataTypes.INTEGER:
                        return int(the_value)
                    elif etl_type == DataTypes.FLOAT:
                        return float(the_value)
                    else:
                        return the_value
                else:
                    # the feature dimension does not correspond, we return the value as is
                    log.debug(f"could convert str value to int or float: {value}")
                    return value
            else:
                # this value does not contain a dimension or is not of the form "value dimension"
                # we simply cast it as much as we can
                log.info(f"could not convert str value to int or float, returning {value}")
                return cast_value(value=value)
        else:
            # this is not a category, not a boolean and not an int/float value
            # we simply cast it as much as we can
            return cast_value(value=value)
