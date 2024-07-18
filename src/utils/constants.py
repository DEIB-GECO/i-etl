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
    }
}

# TODO NELLY: replace this with ontology codes?
#  So that if two hospitals have different columns names, we still identify it as a Phenotypic variables
PHENOTYPIC_VARIABLES = [
    normalize_column_name("DateOfBirth"),
    normalize_column_name("Sex"),
    normalize_column_name("City"),
    normalize_column_name("GestationalAge"),
    normalize_column_name("Etnicity"),  # for BUZZI in UC1
    normalize_column_name("Ethnicity"),  # for tests
    normalize_column_name("Twins"),
    normalize_column_name("Premature"),
    normalize_column_name("BirthMethod")
]


NO_EXAMINATION_COLUMNS = [
    normalize_column_name("line"),
    normalize_column_name("unnamed"),
    normalize_column_name("id"),
    normalize_column_name("sampleBarcode"),
    normalize_column_name("Sampling"),
    normalize_column_name("SampleQuality"),
    normalize_column_name("SamTimeCollected"),
    normalize_column_name("SamTimeReceived"),
    normalize_column_name("TooYoung"),
    normalize_column_name("BIS")
]

BATCH_SIZE = 50

WORKING_DIR = "working-dir"
DEFAULT_DB_CONNECTION = "mongodb://localhost:27017/"
DEFAULT_DB_NAME = "better_default"
TEST_DB_NAME = "better_test"



