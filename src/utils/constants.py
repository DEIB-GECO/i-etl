from enums.HospitalNames import HospitalNames
from enums.TableNames import TableNames
from utils.utils import normalize_column_name

NO_ID = "-1"

ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1.value: {
        TableNames.PATIENT.value: normalize_column_name("id"),
        TableNames.SAMPLE.value: normalize_column_name("SampleBarcode")
    },
    HospitalNames.TEST_H1.value: {
        TableNames.PATIENT.value: normalize_column_name("id")
    },
    HospitalNames.TEST_H2.value: {
        TableNames.PATIENT.value: normalize_column_name("id")
    },
    HospitalNames.TEST_H3.value: {
        TableNames.PATIENT.value: normalize_column_name("id")
    }
}

BATCH_SIZE = 50

WORKING_DIR = "working-dir"
DEFAULT_DB_CONNECTION = "mongodb://localhost:27017/"
DEFAULT_DB_NAME = "better_default"
TEST_DB_NAME = "better_test"



