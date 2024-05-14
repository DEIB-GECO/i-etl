import pandas as pd
import os

from src.database.Database import *
from src.database.TableNames import TableNames
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Examination import Examination
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.utils.setup_logger import log


class ETL:
    def __init__(self, hospital_name, metadata_filepath: str, samples_filepath: str):
        self.hospital_name = hospital_name
        self.metadata_filepath = metadata_filepath
        self.samples_filepath = samples_filepath
        self.database = Database()
        self.patients = []
        self.examinations = []
        self.clinical_records = []
        self.phenotypic_records = []
        self.phenotypic_variables = {}

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
        elif nb_hospitals_with_name == 1:
            # the hospital already exists, no need to do something
            log.info("One hospital labelled '%s' exists: retrieving it.", new_hospital.get_name())
        else:
            raise ValueError("Several hospitals are labelled '%s': stopping here to avoid further problems.", new_hospital.get_hospital_name())

    def __transform_data(self):
        self.__load_metadata_file()
        self.__load_samples_file()
        self.__create_examinations()
        # self.__create_fair_samples()

    def __insert_data(self):
        pass

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
        for index, sample in self.samples.iterrows():
            log.debug("sample type is: %s", type(sample))
            # create patients, one per sample, and add it to self.patients
            self.__create_patient(sample)
            # # create clinical records by associating observations to patients
            # self.__create_clinical_records(sample, patient)
            # # create phenotypic records by associating observations to patients
            # self.__create_phenotypic_records(sample, patient)
        log.debug(self.patients)

    def __create_patient(self, sample):
        # log.debug(sample)
        self.patients.append(Patient(sample["line"], self.samples_filepath))

    def __create_examinations(self):
        #log.debug(sample)
        columns = self.samples.columns.values.tolist()
        for column in columns:
            code = self.__determine_code_from_metadata(column)
            if code is not None:
                status = "registered"
                category = self.__determine_examination_category(column)
                permitted_datatypes = None
                multiple_results_allowed = False
                body_site = None
                self.examinations.append(Examination(code, status, category, permitted_datatypes, multiple_results_allowed, body_site))
            else:
                # TODO: for now, we skip those examinations. Let's see what we can do instead.
                pass
        log.debug(self.examinations)

    def __determine_code_from_metadata(self, column_name: str):
        # log.debug(self.metadata)
        rows = self.metadata.loc[self.metadata['name'] == column_name]
        if len(rows) == 1:
            log.info("Found exactly an ontology code for the column '%s'", column_name)
            ontology = rows["ontology"].iloc[0]
            code = rows["ontology_code"].iloc[0]
            display = rows["description"].iloc[0]
            cc_tuple = (str(ontology), str(code), str(display))
            return CodeableConcept(cc_tuple)
        elif len(rows) == 0:
            log.warn("Found no ontology code for the column '%s'", column_name)
            return None
        else:
            log.warn("Found several ontology code for the columns '%s'", column_name)
            return None

    def __determine_examination_category(self, column_name: str):
        return column_name in self.phenotypic_variables
    def __create_clinical_records(self, sample):
        pass

    def __create_phenotypic_records(self, sample):
        pass
