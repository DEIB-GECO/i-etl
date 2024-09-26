from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.TableNames import TableNames

NO_ID = "-1"

DELIMITER_PATIENT_ID = ":"

ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id"),
        TableNames.SAMPLE_RECORD: MetadataColumns.normalize_name("SampleBarcode")
    },
    HospitalNames.RS_IMGGE: {
        TableNames.PATIENT: MetadataColumns.normalize_name("IMGGI ID")
    },
    HospitalNames.ES_HSJD: {
        TableNames.PATIENT: MetadataColumns.normalize_name("patient_id")
    },
    HospitalNames.TEST_H1: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id")
    },
    HospitalNames.TEST_H2: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id")
    },
    HospitalNames.TEST_H3: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id")
    }
}