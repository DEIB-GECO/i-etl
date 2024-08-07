import re

from enums.HospitalNames import HospitalNames
from enums.TableNames import TableNames
from utils.utils import normalize_column_name

NO_ID = "-1"

ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1: {
        TableNames.PATIENT: normalize_column_name("id"),
        TableNames.SAMPLE: normalize_column_name("SampleBarcode")
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

DATASET_LOCALES = {
    # use-case 1
    HospitalNames.IT_BUZZI_UC1: "it_IT",
    HospitalNames.RS_IMGGE: "sr_RS",
    HospitalNames.ES_HSJD: "es_ES",
    # use-case 2
    HospitalNames.ES_LAFE: "es_ES",
    HospitalNames.IL_HMC: "en_IL",
    # use-case 3
    HospitalNames.IT_BUZZI_UC3: "it_IT",
    HospitalNames.ES_TERRASSA: "es_ES",
    HospitalNames.DE_UKK: "de_DE"
}


BATCH_SIZE = 50

WORKING_DIR = "working-dir"
DEFAULT_DB_CONNECTION = "mongodb://localhost:27017/"
DEFAULT_DB_NAME = "better_default"
TEST_DB_NAME = "better_test"

PATTERN_VALUE_DIMENSION = re.compile(r'^ *([0-9]+) *([a-zA-Z_-]+) *$')  # we add start and end delimiters (^ and $) to not process cells with multiples values inside

DELIMITER_PATIENT_ID = ":"

