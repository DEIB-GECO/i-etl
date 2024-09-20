import os

from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log

if __name__ == '__main__':
    root = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/datasets/data/BUZZI"
    diagnoses = read_tabular_file_as_string(os.path.join(root, "diagnoses-cleaned.xlsx"))
    regexes = read_tabular_file_as_string(os.path.join(root, "diagnosis-regexes.xlsx"))

    log.info(diagnoses)
    log.info(regexes)

    distinct_diseases_carrier_patient = list(diagnoses["carrier"].unique())
    log.info(distinct_diseases_carrier_patient)
    distinct_diseases_affected_patient = list(diagnoses["affetto"].unique())
    log.info(distinct_diseases_affected_patient)
    distinct_diseases_patients = set(distinct_diseases_carrier_patient + distinct_diseases_affected_patient)
    log.info(distinct_diseases_patients)

    known_diseases = set(regexes["Acronym"].unique())
    log.info(known_diseases)

    patient_disease_not_in_regex = distinct_diseases_patients.difference(known_diseases)
    log.info(patient_disease_not_in_regex)
    regex_disease_not_in_any_patient = known_diseases.difference(distinct_diseases_patients)
    log.info(regex_disease_not_in_any_patient)
