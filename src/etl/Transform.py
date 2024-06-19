import math

from src.datatypes.Identifier import Identifier
from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.datatypes.CodeableConcept import CodeableConcept
from src.datatypes.Reference import Reference
from src.profiles.Examination import Examination
from src.profiles.ExaminationRecord import ExaminationRecord
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Sample import Sample
from src.utils.ExaminationCategory import ExaminationCategory
from src.utils.HospitalNames import HospitalNames
from src.utils.TableNames import TableNames
from src.utils.utils import normalize_value, is_in_insensitive, is_not_nan, \
    get_ontology_system, is_equal_insensitive, convert_value
from src.utils.constants import NONE_VALUE, ID_COLUMNS, PHENOTYPIC_VARIABLES, NO_EXAMINATION_COLUMNS, BATCH_SIZE
from src.utils.setup_logger import log
from src.utils.Counter import Counter


class Transform:

    def __init__(self, extract: Extract, load: Load, database: Database, config: BetterConfig):
        self.extract = extract
        self.load = load
        self.database = database
        self.config = config
        self.counter = Counter()

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

    def run(self):
        self.set_resource_counter_id()
        self.create_hospital(hospital_name=self.config.get_hospital_name())
        self.load.load_json_in_table(table_name=TableNames.HOSPITAL.value, unique_variables=["name"])
        log.info("Hospital count: %s", self.database.count_documents(table_name=TableNames.HOSPITAL.value, filter_dict={}))
        self.create_examinations()
        self.load.load_json_in_table(table_name=TableNames.EXAMINATION.value, unique_variables=["code"])
        log.info("Examination count: %s", self.database.count_documents(table_name=TableNames.EXAMINATION.value, filter_dict={}))
        self.create_samples()
        self.create_patients()
        self.create_examination_records()

    def set_resource_counter_id(self) -> None:
        max_value = -1
        for table_name in TableNames:
            if table_name.value == TableNames.PATIENT.value or table_name.value == TableNames.SAMPLE.value:
                # pass because Patient and Sample resources have their ID assigned by hospitals, not the FAIRificator
                pass
            else:
                current_max_identifier = self.database.get_max_value(table_name=table_name.value, field="identifier.value")
                if current_max_identifier is not None:
                    if current_max_identifier > max_value:
                        max_value = current_max_identifier
                else:
                    # the table is not created yet (this happens when we start from a fresh new DB, thus we skip this it)
                    pass
        # Resource.set_counter(max_value + 1)  # start 1 after the current counter to avoid resources with the same ID
        if max_value > -1:
            log.debug("will set the counter with %s", max_value)
            self.counter.set(new_value=max_value)

    def create_hospital(self, hospital_name: str) -> None:
        log.info("create hospital instance in memory")
        new_hospital = Hospital(id_value=NONE_VALUE, name=hospital_name, counter=self.counter)
        self.hospitals.append(new_hospital)
        self.database.write_in_file(data_array=self.hospitals, table_name=TableNames.HOSPITAL.value, count=1)

    def create_examinations(self):
        log.info("create examination instances in memory")
        columns = self.extract.data.columns.values.tolist()
        count = 1
        for column_name in columns:
            lower_column_name = column_name.lower()
            if lower_column_name not in NO_EXAMINATION_COLUMNS:
                cc = self.create_codeable_concept_from_column(column_name=lower_column_name)
                if cc is not None and cc.codings != []:
                    category = self.determine_examination_category(column_name=lower_column_name)
                    new_examination = Examination(id_value=NONE_VALUE, code=cc, category=category, permitted_data_types=[], counter=self.counter)
                    # log.info("adding a new examination about %s: %s", cc.text, new_examination)
                    self.examinations.append(new_examination)
                    if len(self.examinations) >= BATCH_SIZE:
                        self.database.write_in_file(data_array=self.examinations, table_name=TableNames.EXAMINATION.value, count=count)
                        self.examinations = []
                        count = count + 1
                else:
                    # This should never happen as all variables will end up to have a code
                    # TODO Nelly: this is not true: TooYoung, AnswerIX and BIS have no ontology code as of today (May, 29th 2024)
                    pass
            else:
                log.debug("I am skipping column %s because it has been marked as not being part of examination instances.", lower_column_name)
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        self.database.write_in_file(data_array=self.examinations, table_name=TableNames.EXAMINATION.value, count=count)

    def create_samples(self):
        if is_in_insensitive(value=ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.SAMPLE.value], list_of_compared=self.extract.data.columns):
            # this is a dataset with samples
            log.info("create sample instances in memory")
            created_sample_barcodes = set()
            count = 1
            for index, row in self.extract.data.iterrows():
                for column_name, value in row.items():
                    if value is None or value == "" or not is_not_nan(value):
                        # there is no value for that (sample) column, thus we skip it
                        pass
                    else:
                        sample_barcode = row["samplebarcode"]
                        if sample_barcode not in created_sample_barcodes:
                            created_sample_barcodes.add(sample_barcode)
                            # TODO Nelly: write a for loop based on SAMPLE_VARIABLES, instead of writing it by hand?
                            sampling = row["sampling"] if "sampling" in row else None
                            sample_quality = row["samplequality"] if "samplequality" in row else None
                            time_collected = convert_value(value=row["samtimecollected"]) if "samtimecollected" in row else None
                            time_received = convert_value(value=row["samtimereceived"]) if "samtimereceived" in row else None
                            too_young = convert_value(value=row["tooyoung"]) if "tooyoung" in row else None
                            bis = convert_value(value=row["bis"]) if "bis" in row else None
                            new_sample = Sample(sample_barcode, sampling=sampling, quality=sample_quality,
                                                time_collected=time_collected, time_received=time_received,
                                                too_young=too_young, bis=bis, counter=self.counter)
                            created_sample_barcodes.add(sample_barcode)
                            self.samples.append(new_sample)
                            if len(self.samples) >= BATCH_SIZE:
                                self.database.write_in_file(data_array=self.samples, table_name=TableNames.SAMPLE.value, count=count)
                                self.samples = []
                                count = count + 1
                                # no need to load Sample instances because they are referenced using their ID,
                                # which was provided by the hospital (thus is known by the dataset)
            self.database.write_in_file(data_array=self.samples, table_name=TableNames.SAMPLE.value, count=count)

    def create_examination_records(self):
        log.info("create examination record instances in memory")

        # a. load some data from the database to compute references
        self.mapping_hospital_to_hospital_id = self.load.retrieve_identifiers(table_name=TableNames.HOSPITAL.value,
                                                                              projection="name")
        log.debug(self.mapping_hospital_to_hospital_id)

        self.mapping_column_to_examination_id = self.load.retrieve_identifiers(table_name=TableNames.EXAMINATION.value,
                                                                               projection="code.text")
        log.debug(self.mapping_column_to_examination_id)

        # b. Create ExaminationRecord instance
        count = 1
        for index, row in self.extract.data.iterrows():
            # create examination records by associating observations to patients (and possibly the sample)
            for column_name, value in row.items():
                lower_column_name = column_name.lower()
                if lower_column_name in NO_EXAMINATION_COLUMNS or value is None or value == "" or not is_not_nan(value):
                    # (i) either, there is no examination for that column
                    # (ii) or, there is no value for that examination
                    # if both cases, no need to create an ExaminationRecord instance
                    pass
                else:
                    if lower_column_name in self.mapping_column_to_examination_id:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that examination
                        examination_id = self.mapping_column_to_examination_id[lower_column_name]
                        examination_ref = Reference(resource_identifier=examination_id, resource_type=TableNames.EXAMINATION.value)
                        hospital_id = self.mapping_hospital_to_hospital_id[HospitalNames.IT_BUZZI_UC1.value]
                        hospital_ref = Reference(resource_identifier=hospital_id, resource_type=TableNames.HOSPITAL.value)
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        patient_id = Identifier(id_value=row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.PATIENT.value]], resource_type=TableNames.PATIENT.value)  # TODO Nelly: Replace BUZZI by the current hospital
                        subject_ref = Reference(resource_identifier=patient_id.value, resource_type=TableNames.PATIENT.value)
                        sample_id = Identifier(id_value=row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.SAMPLE.value]], resource_type=TableNames.SAMPLE.value)  # TODO Nelly: Replace BUZZI by the current hospital
                        sample_ref = Reference(resource_identifier=sample_id.value, resource_type=TableNames.SAMPLE.value)
                        # TODO Pietro: we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        new_examination_record = ExaminationRecord(id_value=NONE_VALUE, examination_ref=examination_ref,
                                                                   subject_ref=subject_ref, hospital_ref=hospital_ref,
                                                                   sample_ref=sample_ref, value=fairified_value,
                                                                   counter=self.counter)
                        self.examination_records.append(new_examination_record)
                        if len(self.examination_records) >= BATCH_SIZE:
                            self.database.write_in_file(data_array=self.examination_records, table_name=TableNames.EXAMINATION_RECORD.value, count=count)
                            # no need to load ExaminationRecords instances because they are never referenced
                            self.examination_records = []
                            count = count + 1
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        # TODO Nelly: this is not true, 3 columns in Buzzi are still not mapped
                        pass

        self.database.write_in_file(data_array=self.examination_records, table_name=TableNames.EXAMINATION_RECORD.value, count=count)

    def create_patients(self):
        log.info("create patient instances in memory")
        created_patient_ids = set()
        count = 1
        for index, row in self.extract.data.iterrows():
            patient_id = row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.PATIENT.value]]
            if patient_id not in created_patient_ids:
                # the patient does not exist yet, we will create it
                new_patient = Patient(id_value=str(patient_id), counter=self.counter)
                created_patient_ids.add(patient_id)
                self.patients.append(new_patient)
                if len(self.patients) >= BATCH_SIZE:
                    self.database.write_in_file(data_array=self.patients, table_name=TableNames.PATIENT.value, count=count)  # this will save the data if it has reached BATCH_SIZE
                    self.patients = []
                    count = count + 1
                    # no need to load Patient instances because they are referenced using their ID,
                    # which was provided by the hospital (thus is known by the dataset)
        self.database.write_in_file(data_array=self.patients, table_name=TableNames.PATIENT.value, count=count)

    def create_codeable_concept_from_column(self, column_name: str):
        rows = self.extract.metadata.loc[self.extract.metadata['name'] == column_name]
        if len(rows) == 1:
            row = rows.iloc[0]
            cc = CodeableConcept()
            cc_tuple = self.create_code_from_metadata(row=row, ontology_column="ontology", code_column="ontology_code")
            if cc_tuple is not None:
                cc.add_coding(triple=cc_tuple)
            cc_tuple = self.create_code_from_metadata(row=row, ontology_column="secondary_ontology", code_column="secondary_ontology_code")
            if cc_tuple is not None:
                cc.add_coding(triple=cc_tuple)
            cc.text = row["name"]  # the column name (display inside codings will have name+description)
            return cc
        elif len(rows) == 0:
            # log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            # log.warn("Found several times the column '%s' in the metadata", column_name)
            return None

    def create_code_from_metadata(self, row, ontology_column: str, code_column: str):
        ontology = row[ontology_column]
        if not is_not_nan(value=ontology):
            # no ontology code has been provided for that variable name, let's skip it
            return None
        else:
            ontology = get_ontology_system(ontology=ontology)  # get the URI of the ontology system instead of its string name
            code = normalize_value(input_string=row[code_column])  # get the ontology code in the metadata for the given column and normalize it (just in case)
            display = Examination.get_label(row=row)
            # log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            return cc_tuple

    def determine_examination_category(self, column_name: str):
        cc = CodeableConcept()
        if self.is_column_name_phenotypic(column_name=column_name):
            cc.add_coding(triple=ExaminationCategory.CATEGORY_PHENOTYPIC.value)
        else:
            cc.add_coding(triple=ExaminationCategory.CATEGORY_CLINICAL.value)
        return cc

    def is_column_name_phenotypic(self, column_name: str):
        for phen_variable in PHENOTYPIC_VARIABLES:
            if is_equal_insensitive(value=phen_variable, compared=column_name):
                return True
        return False

    def fairify_value(self, column_name, value):
        if column_name in self.extract.mapped_values:
            # we iterate over all the mappings of a given column
            for mapping in self.extract.mapped_values[column_name]:
                # we get the value of the mapping, e.g., F, or M, or NA
                mapped_value = mapping['value']
                # if the sample value is equal to the mapping value, we have found a match,
                # and we will record the associated ontology term instead of the value
                if is_equal_insensitive(value=value, compared=mapped_value):
                    # we create a CodeableConcept with each code added to the mapping, e.g., snomed_ct and loinc
                    # recall that a mapping is of the form: {'value': 'X', 'explanation': '...', 'snomed_ct': '123', 'loinc': '456' }
                    # and we add each ontology code to that CodeableConcept
                    cc = CodeableConcept()
                    for key, val in mapping.items():
                        # for any key value pair that is not about the value or the explanation
                        # (i.e., loinc and snomed_ct columns), we create a Coding, which we add to the CodeableConcept
                        # we need to do a loop because there may be several ontology terms for a single mapping
                        if key != 'value' and key != 'explanation':
                            system = get_ontology_system(ontology=key)
                            code = normalize_value(input_string=val)
                            display = mapping['explanation']
                            cc.add_coding(triple=(system, code, display))
                    return cc  # return the CC computed out of the corresponding mapping
            return convert_value(value=value)  # no coded value for that value, trying at least to normalize it a bit
        return convert_value(value=value)  # no coded value for that value, trying at least to normalize it a bit

