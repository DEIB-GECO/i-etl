import math

import pandas as pd
import os

from numpy import NaN

from src.database.Database import *
from src.database.TableNames import TableNames
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.fhirDatatypes.Reference import Reference
from src.profiles.ClinicalRecord import ClinicalRecord
from src.profiles.Examination import Examination
from src.profiles.ExaminationCategory import ExaminationCategory
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.PhenotypicRecord import PhenotypicRecord
from src.utils.DistributionPlot import DistributionPlot
from src.utils.Utils import is_float
from src.utils.setup_logger import log


class ETL:
    def __init__(self, hospital_name, metadata_filepath: str, samples_filepath: str):
        # self.database = Database(database_name="better_database")
        self.database = Database()
        self.hospital_name = hospital_name
        self.hospital = self.__create_hospital()
        self.metadata_filepath = metadata_filepath
        self.samples_filepath = samples_filepath
        self.patients = []
        self.examinations = []
        self.clinical_records = []
        self.phenotypic_records = []
        self.phenotypic_variables = {}
        self.mapping_colname_to_examination = {}
        self.mapped_values = {}

    def run(self):
        # self.__create_db_indexes()
        # self.__load_phenotypic_variables()
        # self.__transform_data()
        # self.__insert_data()

        # draw insights from the data
        # cursor = self.database.get_min_value_of_phenotypic_record(TableNames.PHENOTYPIC_RECORD_TABLE_NAME, "76")
        # cursor = self.database.get_avg_value_of_phenotypic_record(TableNames.PHENOTYPIC_RECORD_TABLE_NAME, "76")
        cursor = self.database.get_value_distribution_of_examination(TableNames.PHENOTYPIC_RECORD_TABLE_NAME, "76")
        plot = DistributionPlot(cursor, "7")  # do not print the cursor before, otherwise it would consume it
        plot.draw()

    def __create_db_indexes(self):
        self.database.create_unique_index(table_name=TableNames.PATIENT_TABLE_NAME, columns={"metadata.csv_filepath": 1, "metadata.csv_line": 1})
        self.database.create_unique_index(TableNames.HOSPITAL_TABLE_NAME, {"id": 1, "name": 1})
        self.database.create_unique_index(TableNames.EXAMINATION_TABLE_NAME, {"code.codings.system": 1, "code.codings.code": 1})
        self.database.create_unique_index(TableNames.CLINICAL_RECORD_TABLE_NAME, {"instantiate.reference": 1, "subject.reference": 1, "recorded_by.reference": 1, "value": 1, "issued": 1})
        self.database.create_unique_index(TableNames.PHENOTYPIC_RECORD_TABLE_NAME, {"instantiate.reference": 1, "subject.reference": 1, "recorded_by.reference": 1, "value": 1, "issued": 1})

    def __load_phenotypic_variables(self):
        self.phenotypic_variables = json.load(open(os.path.join("data", "metadata", "phenotypic-variables.json")))
        log.debug("Nb of phenotypic variables: %s", len(self.phenotypic_variables))

    def __create_hospital(self):
        new_hospital = Hospital(self.hospital_name)
        hospital_tuple = new_hospital.to_json()

        # check if this hospital has already been created
        # if so, do not create it again, and get its OD
        # otherwise, create it with a new ID
        filter_hospital_name = {}
        # filter_hospital_name = {
        #     "name": new_hospital.get_name()
        # }
        nb_hospitals_with_name = self.database.count_documents(TableNames.HOSPITAL_TABLE_NAME, filter_hospital_name)
        log.debug("Nb of hospitals with name '%s': %d", new_hospital.get_name(), nb_hospitals_with_name)
        if nb_hospitals_with_name == 0:
            # the hospital does not exist yet, we will create it
            log.info("No hospital labelled '%s' exists: creating it.", new_hospital.get_name())
            self.database.insert_one_tuple(TableNames.HOSPITAL_TABLE_NAME, hospital_tuple)
            return new_hospital
        elif nb_hospitals_with_name == 1:
            # the hospital already exists, no need to do something
            log.info("One hospital labelled '%s' exists: retrieving it.", new_hospital.get_name())
            return new_hospital # TODO: return the hospital from the database
        else:
            raise ValueError("Several hospitals are labelled '%s': stopping here to avoid further problems.", new_hospital.get_hospital_name())

    def __transform_data(self):
        self.__load_metadata_file()
        self.__load_samples_file()
        self.__create_examinations()
        self.__create_fair_samples()

    def __insert_data(self):
        self.database.insert_many_tuples(TableNames.PATIENT_TABLE_NAME, [patient.to_json() for patient in self.patients])
        self.database.insert_many_tuples(TableNames.EXAMINATION_TABLE_NAME, [examination.to_json() for examination in self.examinations])
        self.database.insert_many_tuples(TableNames.CLINICAL_RECORD_TABLE_NAME, [clinical_record.to_json() for clinical_record in self.clinical_records])
        self.database.insert_many_tuples(TableNames.PHENOTYPIC_RECORD_TABLE_NAME, [phenotypic_record.to_json() for phenotypic_record in self.phenotypic_records])

    def __load_metadata_file(self):
        assert os.path.exists(self.metadata_filepath), "The provided metadata file could not be found. Please check the filepath you specify when running this script."

        log.debug("self.metadata_filepath is %s", self.metadata_filepath)

        self.metadata = pd.read_csv(self.metadata_filepath)

        log.debug(self.metadata[0:10])

        # lower case all column names to avoid inconsistencies
        self.metadata['name'] = self.metadata['name'].apply(lambda x: x.lower())

        log.debug(self.metadata['name'])

        # remove splaces in ontology names and codes
        self.metadata["ontology"] = self.metadata["ontology"].apply(lambda x: str(x).replace(" ", ""))
        self.metadata["ontology_code"] = self.metadata["ontology_code"].apply(lambda x: str(x).replace(" ", ""))
        self.metadata["secondary_ontology"] = self.metadata["secondary_ontology"].apply(lambda x: str(x).replace(" ", ""))
        self.metadata["secondary_ontology_code"] = self.metadata["secondary_ontology_code"].apply(lambda x: str(x).replace(" ", ""))
        log.debug(self.metadata['ontology'])

    def __load_samples_file(self):
        assert os.path.exists(self.samples_filepath), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.debug("self.samples_filepath is %s", self.samples_filepath)

        self.samples = pd.read_csv(self.samples_filepath)

        log.debug(self.samples[0:10])

        # lower case all column names to avoid inconsistencies
        self.samples = self.samples.reset_index()
        self.samples.columns = self.samples.columns.str.lower()

        log.debug(list(self.samples.columns))

    def __create_fair_samples(self):
        for index, sample in self.samples.iterrows():
            # create patients, one per sample, and add it to self.patients
            patient = self.__create_patient(sample)
            # create clinical and phenotypic records by associating observations to patients
            for column_name, value in sample.items():
                if value is None or value == "" or (is_float(value) and math.isnan(float(value))):
                    # TODO Pietro: is it okay to skip NaN values?
                    # If not, we are losing one of the main advantages of No-SQL databases
                    # there is no value for that examination for the current patient
                    # therefore w d not create a clinical or phenotypic record and skip this cell
                    pass
                else:
                    # log.info("I'm registering value %s for column %s", value, column_name)
                    if column_name in self.mapping_colname_to_examination:
                        # we know a code for this column, so we can register the value of that examination
                        associated_examination = self.mapping_colname_to_examination[column_name]
                        # TODO Pietro: harmonize values (upper-lower case, dates, etc)
                        #  we should use codes (JSON-styled by Boris) whenever we can
                        #  but for others we need normalization (WP6?)
                        #  we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        if associated_examination.get_category() == ExaminationCategory.PHENOTYPIC.name:
                            self.create_phenotypic_record(self.hospital, patient, associated_examination, value)
                        else:
                            self.create_clinical_record(self.hospital, patient, associated_examination, value)
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        pass
        # TODO Nelly: add unique constraints to avoid duplicates
        log.debug("Nb of patients: %s", len(self.patients))
        log.debug("Nb of clinical records: %s", len(self.clinical_records))
        log.debug("Nb of phenotypic records: %s", len(self.phenotypic_records))

    def __create_patient(self, sample):
        # log.debug(sample)
        new_patient = Patient(sample["line"], self.samples_filepath)
        self.patients.append(new_patient)
        return new_patient

    def create_clinical_record(self, hospital: Hospital, patient: Patient, associated_examination: Examination, value):
        status = ""
        code = ""
        issued = ""
        interpretation = ""
        self.clinical_records.append(ClinicalRecord(associated_examination, status, code, patient, hospital, value, issued, interpretation))

    def create_phenotypic_record(self, hospital: Hospital, patient: Patient, associated_examination: Examination, value):
        status = ""
        code = ""
        issued = ""
        interpretation = ""
        self.phenotypic_records.append(PhenotypicRecord(associated_examination, status, code, patient, hospital, value, issued, interpretation))

    def __create_examinations(self):
        columns = self.samples.columns.values.tolist()
        for column in columns:
            code = self.__determine_code_from_metadata(column)
            if code is not None and code.codings != []:
                status = "registered"
                category = self.__determine_examination_category(code)
                permitted_datatypes = []
                multiple_results_allowed = False
                body_site = ""
                new_examination = Examination(code, status, category, permitted_datatypes, multiple_results_allowed, body_site)
                self.examinations.append(new_examination)
                self.mapping_colname_to_examination[column] = new_examination
            else:
                # This should never happen as all variables will end up to have a code
                pass

        log.debug("Nb of examinations: %s", len(self.examinations))

    def __determine_code_from_metadata(self, column_name: str):
        rows = self.metadata.loc[self.metadata['name'] == column_name]
        if len(rows) == 1:
            cc = CodeableConcept()
            cc_tuple = self.create_code_from_metadata(rows, "ontology", "ontology_code", column_name)
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            cc_tuple = self.create_code_from_metadata(rows, "secondary_ontology", "secondary_ontology_code", column_name)
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            return cc
        elif len(rows) == 0:
            log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            log.warn("Found several times the column '%s' in the metada", column_name)
            return None

    def create_code_from_metadata(self, rows, ontology_column: str, code_column: str, column_name: str):
        ontology = rows[ontology_column].iloc[0]
        if is_float(ontology) and math.isnan(float(ontology)):
            # no ontology code has been provided for that variable name, let's skip it
            return None
        else:
            code = rows[code_column].iloc[0]
            display = rows["name"].iloc[0]
            if not is_float(rows["description"].iloc[0]) or (
                    is_float(rows["description"].iloc[0]) and not math.isnan(float(rows["description"].iloc[0]))):
                # by default the display is the variable name
                # if we also have a description, we append it to the display
                # e.g., "BTD. human biotinidase activity."
                display = display + ". " + str(rows["description"].iloc[0]) + "."
            log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            return cc_tuple

    def __determine_examination_category(self, code: CodeableConcept):
        for coding in code.codings:
            coding_full_name = coding.system + "/" + coding.code
            if coding_full_name in self.phenotypic_variables:
                return ExaminationCategory.PHENOTYPIC.name
            else:
                return ExaminationCategory.CLINICAL.name
