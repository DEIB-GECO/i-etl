from src.database.TableNames import TableNames
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.fhirDatatypes.Reference import Reference
from src.profiles.Examination import Examination
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Resource import Resource


class PhenotypicRecord(Resource):
    ID_COUNTER = 1

    def __init__(self, examination: Examination, status: str, code: CodeableConcept, subject: Patient,
                 hospital: Hospital, value, issued, interpretation: str):
        """
        Create a new ClinicalRecord instance.
        :param examination: the Examination instance that record is referring to.
        :param status: a value in [registered, preliminary, final, amended] depicting the current record status.
        :param code: a CodeableConcept TODO: what is code?
        :param subject: the Patient on which the clinical record has been measured.
        :param hospital: the Hospital in which the clinical record has been measured.
        :param value: the value of what is examined in that clinical record.
        :param issued: when the clinical record has been measured.
        :param interpretation: a CodeableConcept to help understand whether the value is normal or not.
        """
        super().__init__()
        self.url = PhenotypicRecord.ID_COUNTER
        PhenotypicRecord.ID_COUNTER = PhenotypicRecord.ID_COUNTER + 1
        self.instantiate = Reference(examination)
        self.status = status
        self.code = code
        self.subject = Reference(subject)
        self.recorded_by = Reference(hospital)
        self.value = value
        self.issued = issued
        self.interpretation = interpretation

    def get_url(self):
        return self.url

    def get_instantiate(self):
        return self.instantiate

    def get_value(self):
        return self.value

    def get_subject(self):
        return self.subject

    def get_recorded_by(self):
        return self.recorded_by

    def get_status(self):
        return self.status

    def get_code(self):
        return self.code

    def get_issued(self):
        return self.issued

    def get_resource_type(self):
        return TableNames.CLINICAL_RECORD_TABLE_NAME

    def to_json(self):
        json_clinical_record = {
            "url": str(self.url),
            "instantiate": self.instantiate.to_json(),
            "status": str(self.status),
            # "code": self.code.to_json(), # TODO Nelly: add this after understanding what is code about
            "subject": self.subject.to_json(),
            "recorded_by": self.recorded_by.to_json(),
            "value": str(self.value),
            "issued": str(self.issued),
            "interpretation": str(self.interpretation)
        }

        return json_clinical_record
