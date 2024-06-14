from src.datatypes.CodeableConcept import CodeableConcept
from src.datatypes.Reference import Reference
from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter


class DiseaseRecord(Resource):
    def __init__(self, id_value: str, clinical_status: str, subject_ref: Reference, hospital_ref: Reference,
                 disease_ref: Reference, severity: CodeableConcept, recorded_date: str, counter: Counter):
        # set up the resource ID
        super().__init__(id_value, self.get_type(), counter=counter)

        # set up the resource attributes
        self.clinical_status = clinical_status
        self.recorded_date = recorded_date
        self.severity = severity
        self.subject = subject_ref
        self.recorded_by = hospital_ref
        self.instantiate = disease_ref

    def get_type(self):
        return TableNames.DISEASE_RECORD.value

    def to_json(self):
        json_disease_record = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "clinicalStatus": self.clinical_status,
            "recordedDate": self.recorded_date,
            "severity": self.severity.to_json(),
            "subject": self.subject.to_json(),
            "recordedBy": self.recorded_by.to_json(),
            "instantiates": self.instantiate.to_json()
        }

        return json_disease_record
