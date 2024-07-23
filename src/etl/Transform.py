import copy
from datetime import datetime

from pandas import DataFrame

from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.Identifier import Identifier
from datatypes.Reference import Reference
from enums.SampleColumns import SampleColumns
from profiles.LaboratoryFeature import LaboratoryFeature
from profiles.LaboratoryRecord import LaboratoryRecord
from profiles.Hospital import Hospital
from profiles.Patient import Patient
from profiles.Sample import Sample
from utils.Counter import Counter
from enums.ExaminationCategory import ExaminationCategory
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from utils.constants import NO_ID, NO_EXAMINATION_COLUMNS, BATCH_SIZE, ID_COLUMNS, PHENOTYPIC_VARIABLES
from utils.setup_logger import log
from utils.utils import is_not_nan, normalize_ontology_code, cast_value, write_in_file, get_categorical_value_display


class Transform:

    def __init__(self, database: Database, execution: Execution, data: DataFrame, metadata: DataFrame, mapped_values: dict):
        self.database = database
        self.execution = execution
        self.counter = Counter()

        # get data, metadata and the mapped values computed in the Extract step
        self.data = data
        self.metadata = metadata
        self.mapped_values = mapped_values

        # to record objects that will be further inserted in the database
        self.hospitals = []
        self.patients = []
        self.examinations = []
        self.examination_records = []
        self.samples = []

        # to keep track off what is in the CSV and which objects (their identifiers) have been created out of it
        # this will also allow us to get resource identifiers of the referred resources
        self.mapping_hospital_to_hospital_id = {}  # map the hospital names to their IDs
        self.mapping_column_to_examination_id = {}  # map the column names to their Examination IDs
        self.mapping_disease_to_disease_id = {}  # map the disease names to their Disease IDs

    def run(self) -> None:
        self.set_resource_counter_id()
        self.create_hospital(hospital_name=self.execution.hospital_name)
        log.info(f"Hospital count: {self.database.count_documents(table_name=TableNames.HOSPITAL.value, filter_dict={})}")
        self.create_examinations()
        log.info(f"Examination count: {self.database.count_documents(table_name=TableNames.LABORATORY_FEATURE.value, filter_dict={})}")
        self.create_samples()
        self.create_patients()
        self.create_examination_records()

    def set_resource_counter_id(self) -> None:
        self.counter.set_with_database(database=self.database)

    def create_hospital(self, hospital_name: str) -> None:
        log.info(f"create hospital instance in memory")
        new_hospital = Hospital(id_value=NO_ID, name=hospital_name, counter=self.counter)
        self.hospitals.append(new_hospital)
        write_in_file(resource_list=self.hospitals, current_working_dir=self.execution.working_dir_current, table_name=TableNames.HOSPITAL.value, count=1)
        self.database.load_json_in_table(table_name=TableNames.HOSPITAL.value, unique_variables=["name"])

    def create_examinations(self) -> None:
        log.info(f"create examination instances in memory")
        count = 1
        index_for_column_name = self.metadata.columns.get_loc(MetadataColumns.COLUMN_NAME.value)
        log.debug(f"The index of {MetadataColumns.COLUMN_NAME.value} is {index_for_column_name}")
        for row_index, row in self.metadata.iterrows():
            column_name = row[MetadataColumns.COLUMN_NAME.value]  # this has to be accessed with int indexes, not column str name
            log.debug(f"Row about {column_name}")
            if column_name not in NO_EXAMINATION_COLUMNS:
                cc = self.create_codeable_concept_from_column(column_name=column_name)
                log.debug(cc)
                if cc is not None:
                    # some columns have no attributed ontology code
                    # we still add the codeable_concept as an examination
                    # but, it will have only the text (such as "BIS", or "TooYoung") and no associated codings
                    category = Transform.determine_examination_category(column_name=column_name)
                    new_examination = LaboratoryFeature(id_value=NO_ID, code=cc, category=category, permitted_datatype="", counter=self.counter)
                    log.info("adding a new examination about %s: %s", cc.text, new_examination)
                    self.examinations.append(new_examination)
                    if len(self.examinations) >= BATCH_SIZE:
                        write_in_file(resource_list=self.examinations, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_FEATURE.value, count=count)
                        self.examinations = []
                        count = count + 1
                else:
                    # the column was not present in the data, or it was present several times
                    # this should never happen
                    pass
            else:
                log.debug(f"I am skipping column {column_name} because it has been marked as not being part of examination instances.")
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        write_in_file(resource_list=self.examinations, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_FEATURE.value, count=count)
        self.database.load_json_in_table(table_name=TableNames.LABORATORY_FEATURE.value, unique_variables=["code"])

    def create_samples(self) -> None:
        if ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.SAMPLE.value] in self.data.columns:
            # this is a dataset with samples
            log.info(f"create sample instances in memory")
            created_sample_barcodes = set()
            count = 1
            for index, row in self.data.iterrows():
                sample_barcode = row[SampleColumns.SAMPLE_BAR_CODE.value]
                if sample_barcode not in created_sample_barcodes:
                    sampling = row[SampleColumns.SAMPLING.value] if SampleColumns.SAMPLING.value in row else None
                    sample_quality = row[SampleColumns.SAMPLE_QUALITY.value] if SampleColumns.SAMPLE_QUALITY.value in row else None
                    time_collected = cast_value(value=row[SampleColumns.SAMPLE_COLLECTED.value]) if SampleColumns.SAMPLE_COLLECTED.value in row else None
                    time_received = cast_value(value=row[SampleColumns.SAMPLE_RECEIVED.value]) if SampleColumns.SAMPLE_RECEIVED.value in row else None
                    too_young = cast_value(value=row[SampleColumns.SAMPLE_TOO_YOUNG.value]) if SampleColumns.SAMPLE_TOO_YOUNG.value in row else None
                    bis = cast_value(value=row[SampleColumns.SAMPLE_BIS.value]) if SampleColumns.SAMPLE_BIS.value in row else None
                    new_sample = Sample(sample_barcode, sampling=sampling, quality=sample_quality,
                                        time_collected=time_collected, time_received=time_received,
                                        too_young=too_young, bis=bis, counter=self.counter)
                    created_sample_barcodes.add(sample_barcode)
                    self.samples.append(new_sample)
                    if len(self.samples) >= BATCH_SIZE:
                        write_in_file(resource_list=self.samples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.SAMPLE.value, count=count)
                        self.samples = []
                        count = count + 1
                        # no need to load Sample instances because they are referenced using their ID,
                        # which was provided by the hospital (thus is known by the dataset)
            write_in_file(resource_list=self.samples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.SAMPLE.value, count=count)
            log.debug(f"Created distinct {len(created_sample_barcodes)} samples in total.")
        else:
            log.debug("This hospital should not have samples. Skipping it.")

    def create_examination_records(self) -> None:
        log.info(f"create examination record instances in memory")

        # a. load some data from the database to compute references
        self.mapping_hospital_to_hospital_id = self.database.retrieve_identifiers(table_name=TableNames.HOSPITAL.value, projection="name")
        log.debug(f"{self.mapping_hospital_to_hospital_id}")

        self.mapping_column_to_examination_id = self.database.retrieve_identifiers(table_name=TableNames.LABORATORY_FEATURE.value, projection="code.text")
        log.debug(f"{self.mapping_column_to_examination_id}")

        # b. Create ExaminationRecord instance, and write them in temporary (JSON) files
        count = 1
        for index, row in self.data.iterrows():
            # create ExaminationRecord instances by associating observations to patients (and possibly the sample)
            for column_name, value in row.items():
                log.debug(f"for row {index} (type: {type(index)} and column {column_name} (type: {type(column_name)}), value is {value}")
                if value is None or value == "" or not is_not_nan(value):
                    # if there is no value for that examination, no need to create an ExaminationRecord instance
                    pass
                else:
                    if column_name in self.mapping_column_to_examination_id:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that examination
                        examination_id = self.mapping_column_to_examination_id[column_name]
                        examination_ref = Reference(resource_identifier=examination_id, resource_type=TableNames.LABORATORY_FEATURE.value)
                        hospital_id = self.mapping_hospital_to_hospital_id[self.execution.hospital_name]
                        hospital_ref = Reference(resource_identifier=hospital_id, resource_type=TableNames.HOSPITAL.value)
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        # TODO NELLY: if Buzzi decides to remove patient ids, we will have to number them with a Counter
                        #  and to create mappings (as for Hospital and Examination resources)
                        id_column_for_patients = ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT.value]
                        patient_id = Identifier(id_value=row[id_column_for_patients], resource_type=TableNames.PATIENT.value)
                        log.debug(f"patient_id = {patient_id.to_json()} of type {type(patient_id)}")
                        patient_ref = Reference(resource_identifier=patient_id.value, resource_type=TableNames.PATIENT.value)
                        log.debug(f"patient_ref = {patient_ref.to_json()} of type {type(patient_ref)}")
                        id_column_for_samples = ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE.value] if TableNames.SAMPLE.value in ID_COLUMNS[self.execution.hospital_name] else ""
                        sample_ref = None
                        if id_column_for_samples != "":
                            sample_id = Identifier(id_value=row[id_column_for_samples], resource_type=TableNames.SAMPLE.value)
                            sample_ref = Reference(resource_identifier=sample_id.value, resource_type=TableNames.SAMPLE.value)
                        # TODO Nelly: we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        new_examination_record = LaboratoryRecord(id_value=NO_ID, examination_ref=examination_ref,
                                                                  subject_ref=patient_ref, hospital_ref=hospital_ref,
                                                                  sample_ref=sample_ref, value=fairified_value,
                                                                  counter=self.counter)
                        self.examination_records.append(new_examination_record)
                        if len(self.examination_records) >= BATCH_SIZE:
                            write_in_file(resource_list=self.examination_records, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_RECORD.value, count=count)
                            # no need to load ExaminationRecords instances because they are never referenced
                            self.examination_records = []
                            count = count + 1
                    else:
                        # this should never happen
                        # this represents the case when a column has not been converted to an Examination resource
                        pass

        write_in_file(resource_list=self.examination_records, current_working_dir=self.execution.working_dir_current, table_name=TableNames.LABORATORY_RECORD.value, count=count)

    def create_patients(self) -> None:
        log.info(f"create patient instances in memory")
        created_patient_ids = set()
        count = 1
        for index, row in self.data.iterrows():
            log.debug(row["id"])
            patient_id = row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.PATIENT.value]]
            log.info(f"patient id in data: {patient_id} of type {type(patient_id)}")
            if patient_id not in created_patient_ids:
                # the patient does not exist yet, we will create it
                new_patient = Patient(id_value=patient_id, counter=self.counter)
                created_patient_ids.add(patient_id)
                self.patients.append(new_patient)
                if len(self.patients) >= BATCH_SIZE:
                    write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current, table_name=TableNames.PATIENT.value, count=count)  # this will save the data if it has reached BATCH_SIZE
                    self.patients = []
                    count = count + 1
                    # no need to load Patient instances because they are referenced using their ID,
                    # which was provided by the hospital (thus is known by the dataset)
        write_in_file(resource_list=self.patients, current_working_dir=self.execution.working_dir_current, table_name=TableNames.PATIENT.value, count=count)

    def create_codeable_concept_from_column(self, column_name: str) -> CodeableConcept | None:
        rows = self.metadata.loc[self.metadata[MetadataColumns.COLUMN_NAME.value] == column_name]
        log.debug(rows)
        if len(rows) == 1:
            row = rows.iloc[0]
            cc = CodeableConcept()
            coding = self.create_coding_from_metadata(row=row, ontology_column=MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value, code_column=MetadataColumns.FIRST_ONTOLOGY_CODE.value)
            if coding is not None:
                cc.add_coding(one_coding=coding)
            coding = self.create_coding_from_metadata(row=row, ontology_column=MetadataColumns.SEC_ONTOLOGY_SYSTEM.value, code_column=MetadataColumns.SEC_ONTOLOGY_CODE.value)
            if coding is not None:
                cc.add_coding(one_coding=coding)
            # NB July 18th 2024: for the text of a CC, this HAS TO be the column name ONLY (and not the column name and the description).
            # this is because we build a mapping between column names (variables) and their identifiers
            # if there is also the description in the text, then no column name would match the CC text.
            # NOPE   cc.text = Examination.get_display(column_name=row[MetadataColumns.COLUMN_NAME.value], column_description=row[MetadataColumns.SIGNIFICATION_EN.value])  # the column name + description
            cc.text = row[MetadataColumns.COLUMN_NAME.value]
            return cc
        elif len(rows) == 0:
            # log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            # log.warn("Found several times the column '%s' in the metadata", column_name)
            return None

    def create_coding_from_metadata(self, row, ontology_column: str, code_column: str) -> Coding | None:
        ontology = row[ontology_column]
        log.debug(f"{ontology}")
        if not is_not_nan(value=ontology):
            # no ontology code has been provided for that variable name, let's skip it
            return None
        else:
            ontology = Ontologies.get_ontology_system(ontology=ontology)  # get the URI of the ontology system instead of its string name
            code = normalize_ontology_code(ontology_code=row[code_column])  # get the ontology code in the metadata for the given column and normalize it (just in case)
            display = LaboratoryFeature.get_display(column_name=row[MetadataColumns.COLUMN_NAME.value], column_description=row[MetadataColumns.SIGNIFICATION_EN.value])

            log.info(f"Found exactly one row for the coding ({ontology}, {code})")
            return Coding(system=ontology, code=code, display=display)

    @classmethod
    def determine_examination_category(cls, column_name: str) -> CodeableConcept:
        if Transform.is_column_name_phenotypic(column_name=column_name):
            return ExaminationCategory.get_phenotypic()
        else:
            return ExaminationCategory.get_clinical()

    @classmethod
    def is_column_name_phenotypic(cls, column_name: str) -> bool:
        return column_name in PHENOTYPIC_VARIABLES

    def fairify_value(self, column_name, value) -> str | float | datetime | CodeableConcept:
        if column_name in self.mapped_values:
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
                            display = get_categorical_value_display(variable_value=value, value_description=mapping['explanation'])
                            cc.add_coding(one_coding=Coding(system=system, code=val, display=display))
                    return cc  # return the CC computed out of the corresponding mapping
            return cast_value(value=value)  # no coded value for that value, trying at least to normalize it a bit
        return cast_value(value=value)  # no coded value for that value, trying at least to normalize it a bit
