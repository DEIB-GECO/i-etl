from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.TableNames import TableNames

NO_ID = "-1"

DELIMITER_RESOURCE_ID = ":"

ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id"),
        TableNames.SAMPLE_RECORD: MetadataColumns.normalize_name("SampleBarcode")
    },
    HospitalNames.RS_IMGGE: {
        TableNames.PATIENT: MetadataColumns.normalize_name("record_id"),
        TableNames.SAMPLE_RECORD: ""
    },
    HospitalNames.ES_HSJD: {
        TableNames.PATIENT: MetadataColumns.normalize_name("Patient ID"),
        TableNames.SAMPLE_RECORD: ""
    },
    HospitalNames.TEST_H1: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id"),
        TableNames.SAMPLE_RECORD: ""
    },
    HospitalNames.TEST_H2: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id"),
        TableNames.SAMPLE_RECORD: ""
    },
    HospitalNames.TEST_H3: {
        TableNames.PATIENT: MetadataColumns.normalize_name("id"),
        TableNames.SAMPLE_RECORD: ""
    }
}