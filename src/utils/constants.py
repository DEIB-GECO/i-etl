from src.utils.HospitalNames import HospitalNames
from src.utils.Ontologies import Ontologies
from src.utils.TableNames import TableNames
from src.utils.utils import get_ontology_resource_uri

NONE_VALUE = "NONE"

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

PHENOTYPIC_VARIABLES = {
    get_ontology_resource_uri(Ontologies.SNOMEDCT.value["url"], "184099003"): "DateOfBirth",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "21112-8"): "DateOfBirth",
    get_ontology_resource_uri(Ontologies.SNOMEDCT.value["url"], "734000001"): "Sex",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "46098-0"): "Sex",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "68997-6"): "City",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "49051-6"): "GestationalAge",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "46463-6"): "Etnicity",
    get_ontology_resource_uri(Ontologies.SNOMEDCT.value["url"], "28030000"): "Twins",
    get_ontology_resource_uri(Ontologies.SNOMEDCT.value["url"], "206167009"): "Premature",
    get_ontology_resource_uri(Ontologies.SNOMEDCT.value["url"], "236973005"): "BirthMethod"
}

SAMPLE_VARIABLES = ["Sampling", "SampleQuality", "SamTimeCollected", "SamTimeReceived", "TooYoung", "BIS"]


# curly braces here specify a set, i.e., set()
# all values here ARE EXPECTED TO BE LOWER CASE to facilitate comparison (and make it efficient)
NO_EXAMINATION_COLUMNS = {"line", "unnamed", "id", "samplebarcode", "sampling", "samplequality", "samtimecollected",
                          "samtimereceived"}

BATCH_SIZE = 50

DEFAULT_CONFIG_FILE = "properties.ini"

DEFAULT_DB_NAME = "better_default"