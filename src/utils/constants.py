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

LOCALES = {
    HospitalNames.IT_BUZZI_UC1.value: "it_IT",
    HospitalNames.RS_IMGGE: "sr_RS",
    HospitalNames.ES_HSJD: "es_ES",
    HospitalNames.IT_BUZZI_UC3: "it_IT",
    HospitalNames.ES_TERRASSA: "es_ES",
    HospitalNames.DE_UKK: "de_DE",
    HospitalNames.ES_LAFE: "es_ES",
    HospitalNames.IL_HMC: "en_IL"
}

PHENOTYPIC_VARIABLES = {
    get_ontology_resource_uri(ontology_system=Ontologies.SNOMEDCT.value["url"], resource_code="184099003"): "DateOfBirth",
    get_ontology_resource_uri(ontology_system=Ontologies.LOINC.value["url"], resource_code="21112-8"): "DateOfBirth",
    get_ontology_resource_uri(ontology_system=Ontologies.SNOMEDCT.value["url"], resource_code="734000001"): "Sex",
    get_ontology_resource_uri(ontology_system=Ontologies.LOINC.value["url"], resource_code="46098-0"): "Sex",
    get_ontology_resource_uri(ontology_system=Ontologies.LOINC.value["url"], resource_code="68997-6"): "City",
    get_ontology_resource_uri(ontology_system=Ontologies.LOINC.value["url"], resource_code="49051-6"): "GestationalAge",
    get_ontology_resource_uri(ontology_system=Ontologies.LOINC.value["url"], resource_code="46463-6"): "Etnicity",
    get_ontology_resource_uri(ontology_system=Ontologies.SNOMEDCT.value["url"], resource_code="28030000"): "Twins",
    get_ontology_resource_uri(ontology_system=Ontologies.SNOMEDCT.value["url"], resource_code="206167009"): "Premature",
    get_ontology_resource_uri(ontology_system=Ontologies.SNOMEDCT.value["url"], resource_code="236973005"): "BirthMethod"
}

SAMPLE_VARIABLES = ["Sampling", "SampleQuality", "SamTimeCollected", "SamTimeReceived", "TooYoung", "BIS"]


# curly braces here specify a set, i.e., set()
# all values here ARE EXPECTED TO BE LOWER CASE to facilitate comparison (and make it efficient)
NO_EXAMINATION_COLUMNS = {"line", "unnamed", "id", "samplebarcode", "sampling", "samplequality", "samtimecollected",
                          "samtimereceived"}

BATCH_SIZE = 50

DEFAULT_CONFIG_FILE = "properties.ini"

DEFAULT_DB_NAME = "better_default"
TEST_DB_NAME = "better_test"
TEST_TABLE_NAME = "test"
