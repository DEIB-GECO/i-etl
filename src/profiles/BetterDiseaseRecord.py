from src.fhirDatatypes.BetterCodeableConcept import BetterCodeableConcept
from src.fhirDatatypes.BetterReference import BetterReference
from src.profiles.BetterResource import BetterResource
from src.utils.TableNames import TableNames


class BetterDiseaseRecord(BetterResource):
    ID_COUNTER = 1

    def __init__(self, id_value: str, clinical_status: str, subject_ref: BetterReference, hospital_ref: BetterReference, disease_ref: BetterReference, severity: BetterCodeableConcept, recorded_date: str):
        # set up the resource ID
        super().__init__(id_value, self.get_type())

        # set up the resource attributes
        self.clinical_status = clinical_status
        self.recorded_by = hospital_ref
        self.subject = subject_ref
        self.instantiate = disease_ref
        self.severity = severity
        self.recorded_date = recorded_date

    def get_type(self):
        return TableNames.DISEASE_RECORD.value

    def to_json(self):
        json_disease_record = {
            "identifier": self.identifier,
            "resourceType": "DiseaseRecord",
            "clinicalStatus": self.clinical_status,
            "recordedBy": self.recorded_by.to_json(),
            "instantiateDisease": self.instantiate.to_json(),
            "subject": self.subject.to_json(),
            "severity": self.severity.to_json(),
            "recordedDate": self.recorded_date
        }

        return json_disease_record
