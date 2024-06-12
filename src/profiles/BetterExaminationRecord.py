from src.fhirDatatypes.BetterCodeableConcept import BetterCodeableConcept
from src.fhirDatatypes.BetterCoding import BetterCoding
from src.fhirDatatypes.BetterReference import BetterReference
from src.profiles.BetterResource import BetterResource
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE


class BetterExaminationRecord(BetterResource):
    ID_COUNTER = 1

    def __init__(self, id_value: str, examination_ref: BetterReference, subject_ref: BetterReference,
                 hospital_ref: BetterReference, sample_ref: BetterReference, value, status: str):
        """
        A new ClinicalRecord instance, either built from existing data or from scratch.
        :param id_value: A string being the BETTER ID of the ExaminationRecord instance.
        :param examination_ref: An Examination instance being the Examination that record is referring to.
        :param subject_ref: A Patient instance being the patient on which the record has been measured.
        :param hospital_ref: A Hospital instance being the hospital in which the record has been measured.
        :param sample_ref: A Sample instance being the sample on which the record has been measured.
        :param status: A string in [registered, preliminary, final, amended] depicting the current record status.
        :param value: A string/int/float/CodeableConcept being the value of what is examined in that record.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.status = status
        self.code = "final"
        self.value = value
        self.recorded_by = hospital_ref
        self.based_on = sample_ref
        self.instantiate = examination_ref
        self.subject = subject_ref

    def get_type(self) -> str:
        return TableNames.EXAMINATION_RECORD.value

    def to_json(self):
        if isinstance(self.value, BetterCodeableConcept) or isinstance(self.value, BetterCoding) or isinstance(self.value, BetterReference):
            # complex type, we need to expand it with .to_json()
            expanded_value = self.value.to_json()
        else:
            # primitive type, no need to expand it
            expanded_value = self.value

        json_clinical_record = {
            "identifier": self.identifier,
            "resourceType": self.get_type(),
            "status": self.status,
            "code": NONE_VALUE,  # This is defined in the FHIR standard and cannot be optional, thus I set it the None value.
            "value": expanded_value,
            "recordedBy": self.recorded_by.to_json(),
            "basedOn": self.based_on.to_json(),
            "instantiate": self.instantiate.to_json(),
            "subject": self.subject.to_json()
        }

        return json_clinical_record
