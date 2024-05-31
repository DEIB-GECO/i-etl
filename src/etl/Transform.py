from src.database.Database import Database
from src.etl.Extract import Extract
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.fhirDatatypes.Reference import Reference
from src.profiles.Examination import Examination
from src.profiles.ExaminationRecord import ExaminationRecord
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Sample import Sample
from src.utils.ExaminationCategory import ExaminationCategory
from src.utils.HospitalNames import HospitalNames
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE, ID_COLUMNS, PHENOTYPIC_VARIABLES, NO_EXAMINATION_COLUMNS, BATCH_SIZE
from src.utils.setup_logger import log
from src.utils.utils import is_not_nan, get_ontology_system, is_equal_insensitive, normalize_value, \
    convert_value, get_ontology_resource_uri, create_identifier


class Transform:

    def __init__(self, extract: Extract, hospital_name: str, database: Database):
        self.extract = extract
        self.hospital_name = hospital_name
        self.database = database

        # to record objects that will be further inserted in the database
        self.patients = set()
        self.examinations = []
        self.examination_records = []

        # to keep track off what is in the CSV and which objects (their identifiers) have been created out of it
        # this will also allow us to get resource identifiers of the referred resources
        self.mapping_hospital_to_hospital_id = {}  # map the hospital names to their IDs
        self.mapping_column_to_examination_id = {}  # map the column names to their Examination IDs
        self.mapping_disease_to_disease_id = {}  # map the disease names to their Disease IDs

    def run(self):
        self.create_hospital(hospital_name=self.hospital_name)
        self.create_examinations()
        self.create_samples()
        # self.create_examination_records()

    def create_hospital(self, hospital_name: str) -> None:
        log.info("create hospital")
        hospital_name = normalize_value(hospital_name)
        hospital_instance = Hospital(NONE_VALUE, hospital_name)
        filter_hospital_name = ["name"]
        upserted_document = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, unique_variables=filter_hospital_name, one_tuple=hospital_instance.to_json())
        log.debug(upserted_document)
        self.mapping_hospital_to_hospital_id = self.database.retrieve_identifiers(table_name=TableNames.HOSPITAL.value, projection="name")

    def create_examinations(self):
        log.info("create examinations")
        columns = self.extract.data.columns.values.tolist()
        for column in columns:
            if column.lower() not in NO_EXAMINATION_COLUMNS:
                code = self.create_codeable_concept_from_column(column)
                if code is not None and code.codings != []:
                    log.info("adding a new examination about %s.", code.text)
                    status = "registered"
                    category = self.determine_examination_category(code)
                    new_examination = Examination(id_value=NONE_VALUE, code=code, status=status, category=category)
                    self.examinations.append(new_examination)
                    if len(self.examinations) > BATCH_SIZE:
                        self.database.upsert_many_tuples(table_name=TableNames.EXAMINATION.value, unique_variables=["code"], tuples=[examination.to_json() for examination in self.examinations])
                        self.examinations = []
                else:
                    # This should never happen as all variables will end up to have a code
                    # TODO Nelly: this is not true: TooYoung, AnswerIX and BIS have no ontology code as of today (May, 29th 2024)
                    pass
            else:
                log.debug("I am skipping column %s because it has been marked as not being part of examination instances.", column)
        self.mapping_column_to_examination_id = self.database.retrieve_identifiers(table_name=TableNames.EXAMINATION.value, projection="code.text")
        log.debug("Nb of examinations: %s", len(self.examinations))

    def create_samples(self):
        # TODO Nelly: code this
        pass

    def create_examination_records(self):
        log.info("create examination records")
        for index, row in self.extract.data.iterrows():
            # create clinical and phenotypic records by associating observations to patients (and possibly the sample)
            for column_name, value in row.items():
                lower_column_name = column_name.lower()
                if lower_column_name in NO_EXAMINATION_COLUMNS or value is None or value == "" or not is_not_nan(value):
                    # (i) either, there is no examination for that column
                    # (ii) or, there is no value for that examination
                    # if both cases, no need to create an ExaminationRecord instance
                    pass
                else:
                    log.debug(self.mapping_column_to_examination_id)
                    if lower_column_name in self.mapping_column_to_examination_id:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that examination
                        examination_id = self.mapping_column_to_examination_id[lower_column_name]
                        hospital_id = self.mapping_hospital_to_hospital_id[self.hospital_name.lower()]
                        # for patient and sample instances, no need to go through a mapping because they have an ID assigned by the hospital
                        patient_id = row[ID_COLUMNS[HospitalNames.BUZZI.value][TableNames.PATIENT.value]]  # TODO Nelly: Replace BUZZI by the current hospital
                        sample_id = row[ID_COLUMNS[HospitalNames.BUZZI.value][TableNames.SAMPLE.value]]  # TODO Nelly: (fill samples)
                        # TODO Nelly: add the associated sample
                        # TODO Pietro: harmonize values (upper-lower case, dates, etc)
                        #  we should use codes (JSON-styled by Boris) whenever we can
                        #  but for others we need normalization (WP6?)
                        #  we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        fairified_value = self.fairify_value(column_name=column_name, value=value)
                        status = ""
                        new_examination_record = ExaminationRecord(id_value=NONE_VALUE, examination_ref=examination_id,
                                                                   subject_ref=patient_id, hospital_ref=hospital_id,
                                                                   sample_ref=sample_id, value=fairified_value, status=status)
                        self.examination_records.append(new_examination_record)
                        if len(self.examinations) > BATCH_SIZE:
                            self.database.upsert_many_tuples(TableNames.EXAMINATION_RECORD.value, ["recordedBy", "subject", "basedOn", "instantiate"], [examination_record.to_json() for examination_record in self.examination_records])
                            # no need to load ExaminationRecords instances because they are never referenced
                            self.examination_records = []  # TODO Nelly: maybe trigger this from the Load class?
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        # TODO Nelly: this is not true, 3 columns in Buzzi are still not mapped
                        pass
        log.info("Nb of patients: %s", len(self.patients))
        log.info("Nb of examination records: %s", len(self.examination_records))

    def create_patient(self, sample) -> Patient:
        patient_unique_id = create_identifier(str(sample[ID_COLUMNS[self.hospital.name]]), TableNames.PATIENT.value)
        new_patient = None
        # check if this patient has already been created
        # if so, get it from the database, otherwise, create a new Patient
        log.info("I already know %s patients", len(self.extract.existing_local_patient_ids))
        log.info(self.extract.existing_local_patient_ids)
        if patient_unique_id in self.extract.existing_local_patient_ids:
            # the patient already exists, no need to do something
            log.debug("The patient with ID %s already exists and will be retrieved from the database. ")
            filter_patient_id = {"identifier": patient_unique_id}
            cursor = self.database.find_operation(TableNames.PATIENT.value, filter_patient_id, {"identifier.value": 1})
            for result in cursor:
                log.info("A patient with ID %s exists: retrieving it.", patient_unique_id)
                log.info(result["identifier"])
                new_patient = Patient(id_value=result["identifier"])
                log.info(new_patient)
        else:
            # the patient does not exist yet, we will create it
            log.info("No patient with ID %s: creating it.", patient_unique_id)
            log.debug(type(patient_unique_id))
            new_patient = Patient(id_value=patient_unique_id.value)
        self.patients.add(new_patient)
        log.debug(new_patient)
        log.debug(self.patients)
        return new_patient

    def create_codeable_concept_from_column(self, column_name: str):
        rows = self.extract.metadata.loc[self.extract.metadata['name'] == column_name]
        if len(rows) == 1:
            row = rows.iloc[0]
            cc = CodeableConcept()
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
            display = Examination.get_label(row)
            # log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            return cc_tuple

    def determine_examination_category(self, code: CodeableConcept):
        for coding in code.codings:
            coding_full_name = get_ontology_resource_uri(coding.system, coding.code)
            if self.is_column_name_phenotypic(coding_full_name):
                return CodeableConcept(ExaminationCategory.CATEGORY_PHENOTYPIC.value)
            else:
                return CodeableConcept(ExaminationCategory.CATEGORY_CLINICAL.value)

    def is_column_name_phenotypic(self, code_url: str):
        for key, value in PHENOTYPIC_VARIABLES.items():
            if is_equal_insensitive(value=key, compared=code_url):
                return True
        return False

    def fairify_value(self, column_name, value):
        if column_name in self.extract.mapped_values:
            # we iterate over all the mappings of a given column
            for mapping in self.extract.mapped_values[column_name]:
                # we get the value of the mapping, e.g., F, or M, or NA
                mapped_value = mapping['value']
                # if the sample value is equal to the mapping value,
                # we have found a match and we will record the associated ontology term instead of the value
                if is_equal_insensitive(value, mapped_value):
                    # we create a CodeableConcept with each code added to the mapping, e.g., snomed_ct and loinc
                    # recall that a mapping is of the form: {'value': 'X', 'explanation': '...', 'snomed_ct': '123', 'loinc': '456' }
                    # and we add each ontology code to that CodeableConcept
                    cc = CodeableConcept()
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

