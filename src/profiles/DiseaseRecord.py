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
        super().__init__(id_value, TableNames.DISEASE_RECORD.value, counter=counter)

        # set up the resource attributes
        self.clinical_status = clinical_status
        self.recorded_date = recorded_date
        self.severity = severity
        self.subject = subject_ref
        self.recorded_by = hospital_ref
        self.instantiate = disease_ref
