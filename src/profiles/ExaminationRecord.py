from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.fhirDatatypes.Coding import Coding
from src.fhirDatatypes.Reference import Reference
from src.profiles.Examination import Examination
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Resource import Resource
from src.utils.utils import build_url, EXAMINATION_RECORD_TABLE_NAME


class ExaminationRecord(Resource):
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
        self.id = ExaminationRecord.ID_COUNTER
        ExaminationRecord.ID_COUNTER = ExaminationRecord.ID_COUNTER + 1
        self.url = build_url(EXAMINATION_RECORD_TABLE_NAME, self.id)
        self.instantiate = Reference(examination)
        self.status = status
        self.code = code
        self.subject = Reference(subject)
        self.recorded_by = Reference(hospital)
        self.value = value
        self.issued = issued
        self.interpretation = interpretation

    def get_url(self) -> str:
        return self.url

    def get_resource_type(self) -> str:
        return EXAMINATION_RECORD_TABLE_NAME

    def to_json(self):
        expanded_value = None
        if isinstance(self.value, CodeableConcept) or isinstance(self.value, Coding) or isinstance(self.value, Reference):
            # ccomplex type, we need to expand it with .to_json()
            expanded_value = self.value.to_json()
        else:
            # primitive type, no need to expand it
            expanded_value = self.value

        json_clinical_record = {
            "id": self.id,
            "url": self.url,
            "instantiate": self.instantiate.to_json(),
            "status": self.status,
            # "code": self.code.to_json(), # TODO Nelly: add this after understanding what is code about
            "subject": self.subject.to_json(),
            "recorded_by": self.recorded_by.to_json(),
            "value": expanded_value,
            "issued": self.issued,
            "interpretation": self.interpretation
        }

        return json_clinical_record
