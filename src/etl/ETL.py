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
from src.utils.Utils import is_float
from src.utils.setup_logger import log


class ETL:
    def __init__(self, hospital_name, metadata_filepath: str, samples_filepath: str):
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

    def run(self):
        self.__load_phenotypic_variables()
        self.__create_hospital()
        self.__transform_data()
        self.__insert_data()

    def __load_phenotypic_variables(self):
        self.phenotypic_variables = json.load(open(os.path.join("data", "metadata", "phenotypic-variables.json")))
        log.debug(self.phenotypic_variables)

    def __create_hospital(self):
        new_hospital = Hospital(self.hospital_name)
        log.debug(new_hospital)
        log.debug(new_hospital.to_json())
        hospital_tuple = new_hospital.to_json()
        log.debug(hospital_tuple)

        # check if this hospital has already been created
        # if so, do not create it again, and get its OD
        # otherwise, create it with a new ID
        log.debug(new_hospital.get_name())
        filter_hospital_name = {}
        # filter_hospital_name = {
        #     "hospital_name": new_hospital.get_hospital_name()
        # }
        log.debug(filter_hospital_name)
        nb_hospitals_with_name = self.database.count_documents(TableNames.HOSPITAL_TABLE_NAME, filter_hospital_name)
        log.debug(nb_hospitals_with_name)
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
        # self.database.insert_many_tuples(TableNames.PATIENT_TABLE_NAME, [patient.to_json() for patient in self.patients])
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
        log.debug("self.samples is of type %s", type(self.samples))
        log.debug(self.mapping_colname_to_examination)
        for index, sample in self.samples.iterrows():
            log.debug("sample type is: %s", type(sample))
            # create patients, one per sample, and add it to self.patients
            patient = self.__create_patient(sample)
            # create clinical and phenotypic records by associating observations to patients
            log.debug(sample.items())
            for column_name, value in sample.items():
                # log.debug("%s is of type %s", value, type(value))
                if value is None or value == "" or (is_float(value) and math.isnan(float(value))):
                    # TODO Pietro: is it okay to skip NaN values?
                    # If not, we are losing one of the main advantages of No-SQL databases
                    # there is no value for that examination for the current patient
                    # therefore w d not create a clinical or phenotypic record and skip this cell
                    pass
                else:
                    log.info("I'm registering value %s for column %s", value, column_name)
                    if column_name in self.mapping_colname_to_examination:
                        log.debug("in if")
                        associated_examination = self.mapping_colname_to_examination[column_name]
                        if associated_examination.get_category == ExaminationCategory.PHENOTYPIC:
                            self.create_phenotypic_record(self.hospital, patient, associated_examination, value)
                        else:
                            self.create_clinical_record(self.hospital, patient, associated_examination, value)
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        pass
        log.debug(self.patients[0].to_json())
        testrf = Reference(self.patients[0])
        log.debug(testrf.to_json())
        log.debug(self.patients[0])
        log.debug(self.patients)
        log.debug(self.clinical_records)

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
        self.clinical_records.append(PhenotypicRecord(associated_examination, status, code, patient, hospital, value, issued, interpretation))

    def __create_examinations(self):
        #log.debug(sample)
        columns = self.samples.columns.values.tolist()
        for column in columns:
            code = self.__determine_code_from_metadata(column)
            if code is not None:
                status = "registered"
                category = self.__determine_examination_category(column)
                permitted_datatypes = []
                multiple_results_allowed = False
                body_site = ""
                new_examination = Examination(code, status, category, permitted_datatypes, multiple_results_allowed, body_site)
                self.examinations.append(new_examination)
                self.mapping_colname_to_examination[column] = new_examination
            else:
                # This should never happen as all variables will end up to have a code
                pass

        log.info(self.examinations)

    def __determine_code_from_metadata(self, column_name: str):
        # log.debug(self.metadata)
        rows = self.metadata.loc[self.metadata['name'] == column_name]
        if len(rows) == 1:
            ontology = rows["ontology"].iloc[0]
            code = rows["ontology_code"].iloc[0]
            display = rows["description"].iloc[0]
            log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            cc = CodeableConcept(cc_tuple)
            log.debug(cc)
            return cc
        elif len(rows) == 0:
            log.warn("Found no ontology code for the column '%s'", column_name)
            return None
        else:
            log.warn("Found several ontology code for the columns '%s'", column_name)
            return None

    def __determine_examination_category(self, column_name: str):
        if column_name in self.phenotypic_variables:
            return ExaminationCategory.PHENOTYPIC.name
        else:
            return ExaminationCategory.CLINICAL.name
