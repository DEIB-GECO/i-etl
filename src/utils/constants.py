from utils.HospitalNames import HospitalNames
from utils.Ontologies import Ontologies
from utils.TableNames import TableNames

NO_ID = -1

METADATA_VARIABLES = ["ontology", "ontology_code", "ontology_comment", "secondary_ontology", "secondary_ontology_code",
                      "secondary_ontology_comment", "snomed_vartype", "dataset", "name", "description", "vartype",
                      "details", "JSON_values"]

# This expects to have column names IN LOWER CASE
ID_COLUMNS = {
    HospitalNames.IT_BUZZI_UC1.value: {
        TableNames.PATIENT.value: "id",
        TableNames.SAMPLE.value: "samplebarcode"
    }
}

PHENOTYPIC_VARIABLES = ["DateOfBirth", "DateOfBirth", "Sex", "City", "GestationalAge", "Etnicity", "Twins", "Premature", "BirthMethod"]

SAMPLE_VARIABLES = ["Sampling", "SampleQuality", "SamTimeCollected", "SamTimeReceived", "TooYoung", "BIS"]


# curly braces here specify a set, i.e., set()
# all values here ARE EXPECTED TO BE LOWER CASE to facilitate comparison (and make it efficient)
NO_EXAMINATION_COLUMNS = ["line", "unnamed", "id", "samplebarcode", "sampling", "samplequality", "samtimecollected",
                          "samtimereceived"]

BATCH_SIZE = 50

WORKING_DIR = "working-dir"
DEFAULT_DB_CONNECTION = "mongodb://localhost:27017/"
DEFAULT_DB_NAME = "better_default"
TEST_DB_NAME = "better_test"
TEST_TABLE_NAME = "test"
