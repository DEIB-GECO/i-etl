from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.datatypes.BetterCodeableConcept import BetterCodeableConcept
from src.datatypes.BetterReference import BetterReference
from src.profiles.BetterExamination import BetterExamination
from src.profiles.BetterExaminationRecord import BetterExaminationRecord
from src.profiles.BetterHospital import BetterHospital
from src.profiles.BetterPatient import BetterPatient
from src.profiles.BetterSample import BetterSample
from src.utils.ExaminationCategory import ExaminationCategory
from src.utils.HospitalNames import HospitalNames
from src.utils.TableNames import TableNames
from src.utils.utils import normalize_value, is_in_insensitive, cast_value, is_not_nan, create_identifier, \
    get_ontology_system, is_equal_insensitive, convert_value
from src.utils.constants import NONE_VALUE, ID_COLUMNS, PHENOTYPIC_VARIABLES, NO_EXAMINATION_COLUMNS, BATCH_SIZE
from src.utils.setup_logger import log


class Transform:

    def __init__(self, extract: Extract, load: Load, database: Database, config: BetterConfig):
        self.extract = extract
        self.load = load
        self.database = database
        self.config = config

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
        self.create_hospital(hospital_name=self.config.get_hospital_name())
        self.create_examinations()
        self.create_samples()
        self.create_patients()
        self.create_examination_records()

    def create_hospital(self, hospital_name: str) -> None:
        log.info("create hospital")
        new_hospital = BetterHospital(NONE_VALUE, hospital_name)
        self.hospitals.append(new_hospital)
        self.database.write_in_file(self.hospitals, TableNames.HOSPITAL.value, 1)

    def create_examinations(self):
        log.info("create examinations")
        columns = self.extract.data.columns.values.tolist()
        count = 1
        for column_name in columns:
            lower_column_name = column_name.lower()
            if lower_column_name not in NO_EXAMINATION_COLUMNS:
                cc = self.create_codeable_concept_from_column(lower_column_name)
                if cc is not None and cc.codings != []:
                    status = "registered"
                    category = self.determine_examination_category(lower_column_name)
                    new_examination = BetterExamination(id_value=NONE_VALUE, code=cc, status=status, category=category)
                    # log.info("adding a new examination about %s: %s", cc.text, new_examination)
                    self.examinations.append(new_examination)
                    if len(self.examinations) >= BATCH_SIZE:
                        self.database.write_in_file(self.examinations, TableNames.EXAMINATION.value, count)
                        self.examinations = []
                        count = count + 1
                else:
                    # This should never happen as all variables will end up to have a code
                    # TODO Nelly: this is not true: TooYoung, AnswerIX and BIS have no ontology code as of today (May, 29th 2024)
                    pass
            else:
                log.debug("I am skipping column %s because it has been marked as not being part of examination instances.", lower_column_name)
        # save the remaining tuples that have not been saved (because there were less than BATCH_SIZE tuples before the loop ends).
        self.database.write_in_file(self.examinations, TableNames.EXAMINATION.value, count)

    def create_samples(self):
        if is_in_insensitive(ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.SAMPLE.value], self.extract.data.columns):
            # this is a dataset with samples
            log.info("create sample records")
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
                            sampling = row["sampling"]
                            sample_quality = row["samplequality"]
                            time_collected = cast_value(row["samtimecollected"])
                            time_received = cast_value(row["samtimereceived"])
                            too_young = cast_value(row["tooyoung"])
                            bis = cast_value(row["bis"])
                            new_sample = BetterSample(sample_barcode, sampling=sampling, quality=sample_quality,
                                                      time_collected=time_collected, time_received=time_received,
                                                      too_young=too_young, bis=bis)
                            created_sample_barcodes.add(sample_barcode)
                            self.samples.append(new_sample)
                            if len(self.samples) >= BATCH_SIZE:
                                self.database.write_in_file(self.samples, TableNames.SAMPLE.value, count)
                                self.samples = []
                                count = count + 1
                                # no need to load Sample instances because they are referenced using their ID,
                                # which was provided by the hospital (thus is known by the dataset)
            self.database.write_in_file(self.samples, TableNames.SAMPLE.value, count)
            log.info("Nb of samples: %s", len(self.samples))

    def create_examination_records(self):
        log.info("create examination records")

        # a. load some data from the database to compute references
        self.load.load_json_in_table(TableNames.HOSPITAL.value, ["name"])
        self.mapping_hospital_to_hospital_id = self.load.retrieve_identifiers(table_name=TableNames.HOSPITAL.value,
                                                                              projection="name")
        log.debug(self.mapping_hospital_to_hospital_id)

        self.load.load_json_in_table(TableNames.EXAMINATION.value, ["code"])
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
                        examination_ref = BetterReference(examination_id, TableNames.EXAMINATION.value)
                        hospital_id = self.mapping_hospital_to_hospital_id[HospitalNames.IT_BUZZI_UC1.value]
                        hospital_ref = BetterReference(hospital_id, TableNames.HOSPITAL.value)
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        patient_id = create_identifier(row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.PATIENT.value]], TableNames.PATIENT.value)  # TODO Nelly: Replace BUZZI by the current hospital
                        subject_ref = BetterReference(patient_id, TableNames.PATIENT.value)
                        sample_id = create_identifier(row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.SAMPLE.value]], TableNames.SAMPLE.value)  # TODO Nelly: Replace BUZZI by the current hospital
                        sample_ref = BetterReference(sample_id, TableNames.SAMPLE.value)
                        # TODO Nelly: add the associated sample
                        # TODO Pietro: harmonize values (upper-lower case, dates, etc)
                        #  we should use codes (JSON-styled by Boris) whenever we can
                        #  but for others we need normalization (WP6?)
                        #  we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        status = "final"
                        new_examination_record = BetterExaminationRecord(id_value=NONE_VALUE, examination_ref=examination_ref,
                                                                         subject_ref=subject_ref, hospital_ref=hospital_ref,
                                                                         sample_ref=sample_ref, value=fairified_value, status=status)
                        self.examination_records.append(new_examination_record)
                        if len(self.examination_records) >= BATCH_SIZE:
                            self.database.write_in_file(self.examination_records, TableNames.EXAMINATION_RECORD.value, count)
                            # no need to load ExaminationRecords instances because they are never referenced
                            self.examination_records = []
                            count = count + 1
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        # TODO Nelly: this is not true, 3 columns in Buzzi are still not mapped
                        pass

        self.database.write_in_file(self.examination_records, TableNames.EXAMINATION_RECORD.value, count)
        log.info("Nb of patients: %s", len(self.patients))
        log.info("Nb of examination records: %s", len(self.examination_records))

    def create_patients(self):
        log.info("create patients")
        created_patient_ids = set()
        count = 1
        for index, row in self.extract.data.iterrows():
            patient_id = row[ID_COLUMNS[HospitalNames.IT_BUZZI_UC1.value][TableNames.PATIENT.value]]
            if patient_id not in created_patient_ids:
                # the patient does not exist yet, we will create it
                new_patient = BetterPatient(id_value=str(patient_id))
                created_patient_ids.add(patient_id)
                self.patients.append(new_patient)
                if len(self.patients) >= BATCH_SIZE:
                    self.database.write_in_file(self.patients, TableNames.PATIENT.value, count)  # this will save the data if it has reached BATCH_SIZE
                    self.patients = []
                    count = count + 1
                    # no need to load Patient instances because they are referenced using their ID,
                    # which was provided by the hospital (thus is known by the dataset)
        self.database.write_in_file(self.patients, TableNames.PATIENT.value, count)

    def create_codeable_concept_from_column(self, column_name: str):
        rows = self.extract.metadata.loc[self.extract.metadata['name'] == column_name]
        if len(rows) == 1:
            row = rows.iloc[0]
            cc = BetterCodeableConcept()
            cc_tuple = self.create_code_from_metadata(row, "ontology", "ontology_code")
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            cc_tuple = self.create_code_from_metadata(row, "secondary_ontology", "secondary_ontology_code")
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
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
        if not is_not_nan(ontology):
            # no ontology code has been provided for that variable name, let's skip it
            return None
        else:
            ontology = get_ontology_system(ontology)  # get the URI of the ontology system instead of its string name
            code = normalize_value(row[code_column])  # get the ontology code in the metadata for the given column and normalize it (just in case)
            display = BetterExamination.get_label(row)
            # log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            return cc_tuple

    def determine_examination_category(self, column_name: str):
        if self.is_column_name_phenotypic(column_name):
            return BetterCodeableConcept(ExaminationCategory.CATEGORY_PHENOTYPIC.value)
        else:
            return BetterCodeableConcept(ExaminationCategory.CATEGORY_CLINICAL.value)

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
                if is_equal_insensitive(value, mapped_value):
                    # we create a CodeableConcept with each code added to the mapping, e.g., snomed_ct and loinc
                    # recall that a mapping is of the form: {'value': 'X', 'explanation': '...', 'snomed_ct': '123', 'loinc': '456' }
                    # and we add each ontology code to that CodeableConcept
                    cc = BetterCodeableConcept()
                    for key, val in mapping.items():
                        # for any key value pair that is not about the value or the explanation
                        # (i.e., loinc and snomed_ct columns), we create a Coding, which we add to the CodeableConcept
                        # we need to do a loopp because there may be several ontology terms for a single mapping
                        if key != 'value' and key != 'explanation':
                            system = get_ontology_system(key)
                            code = normalize_value(val)
                            display = mapping['explanation']
                            cc.add_coding((system, code, display))
                    return cc  # return the CC computed out of the corresponding mapping
            return convert_value(value)  # no coded value for that value, trying at least to normalize it a bit
        return convert_value(value)  # no coded value for that value, trying at least to normalize it a bit

