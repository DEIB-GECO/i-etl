from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from datatypes.Reference import Reference
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class DiseaseRecord(Resource):
    def __init__(self, id_value: str, clinical_status: str, subject_ref: Reference, hospital_ref: Reference,
                 disease_ref: Reference, severity: CodeableConcept, recorded_date: datetime, counter: Counter):
        # set up the resource ID
        super().__init__(id_value, self.get_type(), counter=counter)

        # set up the resource attributes
        self._clinical_status = clinical_status
        self._recorded_date = recorded_date
        self._severity = severity
        self._subject = subject_ref
        self._recorded_by = hospital_ref
        self._instantiate = disease_ref

    @classmethod
    def get_type(cls) -> str:
        return TableNames.DISEASE_RECORD.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "clinicalStatus": self._clinical_status,
            "recordedDate": get_mongodb_date_from_datetime(current_datetime=self._recorded_date),
            "severity": self._severity.to_json(),
            "subject": self._subject.to_json(),
            "recordedBy": self._recorded_by.to_json(),
            "instantiates": self._instantiate.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
