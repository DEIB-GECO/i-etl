import os

from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log

if __name__ == '__main__':
    root = "/Users/nelly/Documents/boulot/postdoc-polimi/etl/datasets/data/BUZZI"
    diagnoses = read_tabular_file_as_string(os.path.join(root, "diagnoses-latest.xlsx"))
    regexes = read_tabular_file_as_string(os.path.join(root, "ds-transformation-table.xlsx"))
    data = read_tabular_file_as_string(os.path.join(root, "screening.csv"))

    # 0. preliminary loading and extraction
    distinct_diseases_carrier_patient = list(diagnoses["carrier"].unique())
    distinct_diseases_affected_patient = list(diagnoses["affetto"].unique())
    distinct_diseases_patients = set(distinct_diseases_carrier_patient + distinct_diseases_affected_patient)

    known_diseases = set(regexes["Acronym"].unique())

    data_barcodes = list(data["SampleBarcode"].unique())
    diagnosis_barcodes = list(diagnoses["SampleBarcode"].unique())
    total_distinct_nb_barcodes = len(set(data_barcodes).union(set(diagnosis_barcodes)))

    # 1. compute the list of diagnosis that are not present in the diagnosis description file
    patient_disease_not_in_regex = distinct_diseases_patients.difference(known_diseases)
    patient_disease_not_in_regex = [str(elem) for elem in patient_disease_not_in_regex]
    patient_disease_not_in_regex.sort()
    log.info(f"{len(patient_disease_not_in_regex)} patient diagnoses are not described: {patient_disease_not_in_regex}")

    # 2. compute the list of diagnosis that are described in the description file but are not referred in any patient
    regex_disease_not_in_any_patient = known_diseases.difference(distinct_diseases_patients)
    regex_disease_not_in_any_patient = [str(elem) for elem in regex_disease_not_in_any_patient]
    regex_disease_not_in_any_patient.sort()
    log.info(f"{len(regex_disease_not_in_any_patient)} described diagnoses are not in any patient: {regex_disease_not_in_any_patient}")

    # 3. compute the overlap between data SampleBarcode and diagnosis SampleBarCode (list of codes that are in both)
    common_sample_bar_codes = set(data_barcodes).intersection(set(diagnosis_barcodes))
    all_sample_bar_codes = set(data_barcodes).union(set(diagnosis_barcodes))
    common_sample_bar_codes = [str(elem) for elem in common_sample_bar_codes]
    common_sample_bar_codes.sort()
    ratio = round((len(common_sample_bar_codes) / total_distinct_nb_barcodes)*100, 4)
    log.info(f"{len(common_sample_bar_codes)} SampleBarcode are both present in screening and diagnoses, out of {total_distinct_nb_barcodes} unique sampleBarcode ({ratio}%)")

    # 4. compute the list of data SampleBarcode which are not in the diagnosis file
    sample_barcodes_in_data_not_in_diagnoses = set(data_barcodes).difference(diagnosis_barcodes)
    sample_barcodes_in_data_not_in_diagnoses = [str(elem) for elem in sample_barcodes_in_data_not_in_diagnoses]
    sample_barcodes_in_data_not_in_diagnoses.sort()
    ratio = round((len(sample_barcodes_in_data_not_in_diagnoses) / total_distinct_nb_barcodes)*100, 4)
    log.info(f"{len(sample_barcodes_in_data_not_in_diagnoses)} SampleBarcode are in screening but not in diagnoses, out of {total_distinct_nb_barcodes} unique SampleBarcode ({ratio}%)")

    # 5. compute the list of diagnosis SampleBarcode which are not in the data file
    sample_barcodes_in_diagnoses_not_in_data = set(diagnosis_barcodes).difference(data_barcodes)
    sample_barcodes_in_diagnoses_not_in_data = [str(elem) for elem in sample_barcodes_in_diagnoses_not_in_data]
    sample_barcodes_in_diagnoses_not_in_data.sort()
    ratio = round((len(sample_barcodes_in_diagnoses_not_in_data) / total_distinct_nb_barcodes)*100, 4)
    log.info(f"{len(sample_barcodes_in_diagnoses_not_in_data)} SampleBarcode are in diagnoses but not in screening, out of {total_distinct_nb_barcodes} unique SampleBarcode ({ratio}%)")
