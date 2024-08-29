import os
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


BATCH_SIZE = 1000

WORKING_DIR = "working-dir"
DEFAULT_DB_NAME = "better_default"
# these constants have to exactly match the volume paths described in compose.yaml
DOCKER_FOLDER_DATA = "/home/better-deployed/hospital-data"
DOCKER_FOLDER_METADATA = os.path.join(DOCKER_FOLDER_DATA, "metadata")
SERVER_FOLDER_DIAGNOSIS_CLASSIFICATION = os.path.join(DOCKER_FOLDER_DATA, "classification")
SERVER_FOLDER_DIAGNOSIS_REGEXES = os.path.join(DOCKER_FOLDER_DATA, "regexes")
DOCKER_FOLDER_LABORATORY = os.path.join(DOCKER_FOLDER_DATA, "laboratory")
DOCKER_FOLDER_DIAGNOSIS = os.path.join(DOCKER_FOLDER_DATA, "diagnosis")
DOCKER_FOLDER_MEDICINE = os.path.join(DOCKER_FOLDER_DATA, "medicine")
DOCKER_FOLDER_IMAGING = os.path.join(DOCKER_FOLDER_DATA, "imaging")
DOCKER_FOLDER_GENOMIC = os.path.join(DOCKER_FOLDER_DATA, "genomic")
DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS = os.path.join(DOCKER_FOLDER_DATA, "pids")
DOCKER_FOLDER_TEST = os.path.join(DOCKER_FOLDER_DATA, "test")
DB_CONNECTION = "mongodb://mongo:27017/"
TEST_DB_NAME = "better_test"

PATTERN_VALUE_DIMENSION = re.compile(r'^ *([0-9]+) *([a-zA-Z_-]+) *$')  # we add start and end delimiters (^ and $) to not process cells with multiples values inside

DELIMITER_PATIENT_ID = ":"

DEFAULT_CODING_DISPLAY = ""
