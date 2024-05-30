from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.fhirDatatypes.Coding import Coding
from src.fhirDatatypes.Reference import Reference
from src.profiles.Examination import Examination
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Resource import Resource
from src.profiles.Sample import Sample
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE
from src.utils.utils import get_reference_from_json


class ExaminationRecord(Resource):
    ID_COUNTER = 1

    def __init__(self, id_value: str, examination: Examination, status: str, subject: Patient,
                 hospital: Hospital, value, sample: Sample):
        """
        A new ClinicalRecord instance, either built from existing data or from scratch.
        :param id_value: A string being the BETTER ID of the ExaminationRecord instance.
        :param examination: An Examination instance being the Examination that record is referring to.
        :param status: A string in [registered, preliminary, final, amended] depicting the current record status.
        :param subject: A Patient instance being the patient on which the clinical record has been measured.
        :param hospital: A Hospital instance being the hospital in which the clinical record has been measured.
        :param value: A string/int/float/CodeableConcept being the value of what is examined in that clinical record.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.status = status
        self.code = NONE_VALUE
        self.value = value
        self.recorded_by = Reference(hospital)
        self.based_on = Reference(sample)
        self.instantiate = Reference(examination)
        self.subject = Reference(subject)

    def get_type(self) -> str:
        return TableNames.EXAMINATION_RECORD.value

    def to_json(self):
        if isinstance(self.value, CodeableConcept) or isinstance(self.value, Coding) or isinstance(self.value, Reference):
            # ccomplex type, we need to expand it with .to_json()
            expanded_value = self.value.to_json()
        else:
            # primitive type, no need to expand it
            expanded_value = self.value

        json_clinical_record = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "status": self.status,
            "code": NONE_VALUE,  # This is defined in the FHIR standard and cannot be removed, thus I set it the None value.
            "value": expanded_value,
            "recordedBy": self.recorded_by.to_json(),
            "basedOn": self.based_on.to_json(),
            "instantiate": self.instantiate.to_json(),
            "subject": self.subject.to_json()
        }

        return json_clinical_record

    @classmethod
    def from_json(cls, the_json: dict):
        return cls(id_value=the_json["identifier"]["value"],
                    examination=get_reference_from_json(the_json["instantiate"]),
                    status=the_json["status"],
                    subject=get_reference_from_json(the_json["subject"]),
                    hospital=get_reference_from_json(the_json["recordedBy"]),
                    value=the_json["value"],
                    sample=get_reference_from_json(the_json["basedOn"])
                   )
