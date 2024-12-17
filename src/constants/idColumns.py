from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns

NO_ID = -1

DELIMITER_RESOURCE_ID = ":"

PATIENT_ID = "patient_id"
SAMPLE_ID = "base_id"

ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1: {
        PATIENT_ID: MetadataColumns.normalize_name("id"),
        SAMPLE_ID: MetadataColumns.normalize_name("SampleBarcode")
    },
    HospitalNames.RS_IMGGE: {
        PATIENT_ID: MetadataColumns.normalize_name("record_id"),
        SAMPLE_ID: ""
    },
    HospitalNames.ES_HSJD: {
        PATIENT_ID: MetadataColumns.normalize_name("Patient ID"),
        SAMPLE_ID: ""
    },
    HospitalNames.TEST_H1: {
        PATIENT_ID: MetadataColumns.normalize_name("id"),
        SAMPLE_ID: MetadataColumns.normalize_name("sid"),
    },
    HospitalNames.TEST_H2: {
        PATIENT_ID: MetadataColumns.normalize_name("id"),
        SAMPLE_ID: ""
    },
    HospitalNames.TEST_H3: {
        PATIENT_ID: MetadataColumns.normalize_name("id"),
        SAMPLE_ID: ""
    },
    HospitalNames.EXPES_EDA: {
        PATIENT_ID: MetadataColumns.normalize_name("id"),
        SAMPLE_ID: ""
    },
    HospitalNames.EXPES_COVID: {
        PATIENT_ID: MetadataColumns.normalize_name("id"),
        SAMPLE_ID: MetadataColumns.normalize_name("sid")
    },
    HospitalNames.EXPES_KIDNEY: {
        PATIENT_ID: MetadataColumns.normalize_name("individual_id"),
        SAMPLE_ID: MetadataColumns.normalize_name("sample_id")
    }
}