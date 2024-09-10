from enums.HospitalNames import HospitalNames
from enums.TableNames import TableNames
from utils.utils import normalize_column_name

NO_ID = "-1"

DELIMITER_PATIENT_ID = ":"

ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1: {
        TableNames.PATIENT: normalize_column_name("id"),
        TableNames.SAMPLE: normalize_column_name("SampleBarcode")
    },
    HospitalNames.RS_IMGGE: {
        TableNames.PATIENT: normalize_column_name("IMGGI ID")
    },
    HospitalNames.ES_HSJD: {
        TableNames.PATIENT: normalize_column_name("patient_id")
    },
    HospitalNames.TEST_H1: {
        TableNames.PATIENT: normalize_column_name("id")
    },
    HospitalNames.TEST_H2: {
        TableNames.PATIENT: normalize_column_name("id")
    },
    HospitalNames.TEST_H3: {
        TableNames.PATIENT: normalize_column_name("id")
    }
}