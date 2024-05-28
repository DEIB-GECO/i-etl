from src.database.Database import Database
from src.etl.Extract import Extract
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Examination import Examination
from src.profiles.ExaminationRecord import ExaminationRecord
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Sample import Sample
from src.utils.ExaminationCategory import ExaminationCategory
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log
from src.utils.utils import ID_COLUMNS, is_not_nan, get_ontology_system, is_equal_insensitive, normalize_value, \
    NONE_VALUE, convert_value


class Transform():
    def __init__(self, extract: Extract, hospital_name: str, database: Database):
        self.patients = set()  # to avoid duplicated patients when a single patient has many examination records
        self.examinations = []
        self.examination_records = []
        self.mapping_colname_to_examination = {}
        self.extract = extract
        self.database = database
        self.hospital = self.__create_hospital(hospital_name=hospital_name)

    def run(self):
        self.__create_examinations()
        self.__create_fair_samples()

    def __create_hospital(self, hospital_name: str) -> Hospital:
        # check if this hospital has already been created
        # if so, get it from the database, otherwise, create a new Hospital
        hospital_name = normalize_value(hospital_name)
        if hospital_name in self.extract.existing_hospital_names:
            # the hospital already exists, no need to do something
            filter_hospital_name = {"name": hospital_name}
            cursor = self.database.find_operation(TableNames.HOSPITAL.value, filter_hospital_name, {"id": 1})
            count = 0
            for result in cursor:
                count = count + 1
                if count > 1:
                    log.error("Several hospitals have matched the string '%s'. This is not expected. ")
                else:
                    log.info("One hospital labelled '%s' exists: retrieving it.", hospital_name)
                    return Hospital(id_value=str(result["id"]), name=hospital_name)
        else:
            # the hospital does not exist yet, we will create it
            log.info("No hospital labelled '%s' exists: creating it.", hospital_name)
            new_hospital = Hospital(id_value=NONE_VALUE, name=hospital_name)
            self.database.insert_one_tuple(TableNames.HOSPITAL.value, new_hospital.to_json())
            return new_hospital

    def __create_examinations(self):
        columns = self.extract.samples.columns.values.tolist()
        for column in columns:
            code = self.__create_codeable_concept_from_column(column)
            if code is not None and code.codings != []:
                status = "registered"
                category = self.__determine_examination_category(code)
                new_examination = Examination(id_value=NONE_VALUE, code=code, status=status, category=category)  # TODO Nelly: check for duplicates / already existing examinations
                self.examinations.append(new_examination)
                self.mapping_colname_to_examination[column] = new_examination
            else:
                # This should never happen as all variables will end up to have a code
                pass

        log.debug("Nb of examinations: %s", len(self.examinations))

    def __create_fair_samples(self):
        for index, sample in self.extract.samples.iterrows():
            # create patients, one per unique "id", and add it to self.patients
            patient = self.__create_patient(sample)
            # create clinical and phenotypic records by associating observations to patients
            for column_name, value in sample.items():
                if value is None or value == "" or not is_not_nan(value):
                    # there is no value for that examination for the current patient
                    # therefore we do not create an examination record and skip this cell
                    pass
                else:
                    if column_name in self.mapping_colname_to_examination:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that examination
                        associated_examination = self.mapping_colname_to_examination[column_name]
                        # TODO Pietro: harmonize values (upper-lower case, dates, etc)
                        #  we should use codes (JSON-styled by Boris) whenever we can
                        #  but for others we need normalization (WP6?)
                        #  we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        fairified_value = self.__fairify_value(column_name=column_name, value=value)
                        # log.info("I'm registering value %s for column %s from value %s", fairified_value, column_name, value)
                        self.create_examination_record(self.hospital, patient, associated_examination, fairified_value)
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        pass
        log.info("Nb of patients: %s", len(self.patients))
        log.info("Nb of examination records: %s", len(self.examination_records))

    def __create_patient(self, sample):
        patient_unique_id = str(sample[ID_COLUMNS[self.hospital.name]])
        new_patient = None
        # check if this patient has already been created
        # if so, get it from the database, otherwise, create a new Patient
        if patient_unique_id in self.extract.existing_local_patient_ids:
            # the patient already exists, no need to do something
            filter_patient_id = {"id": patient_unique_id}
            cursor = self.database.find_operation(TableNames.PATIENT.value, filter_patient_id, {"id": 1})
            for result in cursor:
                log.info("A patient with ID %s exists: retrieving it.", patient_unique_id)
                new_patient = Patient(id_value=result["id"])
        else:
            # the patient does not exist yet, we will create it
            log.info("No patient with ID %s: creating it.", patient_unique_id)
            log.debug(type(patient_unique_id))
            new_patient = Patient(id_value=patient_unique_id)
        self.patients.add(new_patient)
        return new_patient

    def create_examination_record(self, hospital: Hospital, patient: Patient, associated_examination: Examination, value):
        status = ""
        code = ""
        sample = Sample(NONE_VALUE, TableNames.SAMPLE.value)  # TODO Nelly: fill the sample
        # TODO Nelly: check whether to put NONE_VALUE or not for Examination id
        new_examination_record = ExaminationRecord(id_value=NONE_VALUE, examination=associated_examination, status=status, code=code, subject=patient, hospital=hospital, value=value, sample=sample)
        self.examination_records.append(new_examination_record)

    def __create_codeable_concept_from_column(self, column_name: str):
        rows = self.extract.metadata.loc[self.extract.metadata['name'] == column_name]
        if len(rows) == 1:
            cc = CodeableConcept()
            cc_tuple = self.__create_code_from_metadata(rows, "ontology", "ontology_code")
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            cc_tuple = self.__create_code_from_metadata(rows, "secondary_ontology", "secondary_ontology_code")
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            return cc
        elif len(rows) == 0:
            # log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            # log.warn("Found several times the column '%s' in the metadata", column_name)
            return None

    def __create_code_from_metadata(self, rows, ontology_column: str, code_column: str):
        # log.debug("ontology_column is: " + str(ontology_column))
        # log.debug("code_column is: " + str(code_column))
        ontology = rows[ontology_column].iloc[0]
        if not is_not_nan(ontology):
            # no ontology code has been provided for that variable name, let's skip it
            return None
        else:
            ontology = get_ontology_system(ontology)  # get the URI of the ontology system instead of its string name
            code = normalize_value(rows[code_column].iloc[0])  # get the ontology code in the metadata for the given column and normalize it (just in case)
            display = rows["name"].iloc[0]
            if is_not_nan(rows["description"].iloc[0]):
                # by default the display is the variable name
                # if we also have a description, we append it to the display
                # e.g., "BTD. human biotinidase activity."
                display = display + ". " + str(rows["description"].iloc[0]) + "."
            # log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            return cc_tuple

    def __determine_examination_category(self, code: CodeableConcept):
        for coding in code.codings:
            coding_full_name = coding.system + "/" + coding.code
            if coding_full_name in self.extract.phenotypic_variables:
                return CodeableConcept(ExaminationCategory.CATEGORY_PHENOTYPIC.value)  #  we can't create the CC directly in utils due to circular imoprts
            else:
                return CodeableConcept(ExaminationCategory.CATEGORY_CLINICAL.value)

    def __fairify_value(self, column_name, value):
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

