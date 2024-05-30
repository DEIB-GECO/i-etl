from src.utils.HospitalNames import HospitalNames
from src.utils.Ontologies import Ontologies
from src.utils.TableNames import TableNames
from src.utils.utils import get_ontology_resource_uri

NONE_VALUE = "NONE"

ID_COLUMNS = {
    HospitalNames.BUZZI.value: {
        TableNames.PATIENT.value: "id",
        TableNames.SAMPLE.value: "samplebarcode"
    }
}

NO_EXAMINATION_COLUMNS = ["line", "unnamed", "id", "samplebarcode"]

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

SAMPLE_VARIABLES = {
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "79566-6"): "Sampling",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "57718-9"): "SampleQuality",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "51953-8"): "SamTimeCollected",
    get_ontology_resource_uri(Ontologies.LOINC.value["url"], "63572-2"): "SamTimeReceived",
    "unknown1": "TooYoung",
    "unknown2": "BIS"
}
