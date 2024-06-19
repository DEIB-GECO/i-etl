from datetime import datetime

from src.datatypes.CodeableConcept import CodeableConcept
from src.datatypes.Coding import Coding
from src.datatypes.Reference import Reference
from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter
from src.utils.utils import get_mongodb_date_from_datetime
from src.utils.setup_logger import log


class ExaminationRecord(Resource):
    def __init__(self, id_value: str, examination_ref: Reference, subject_ref: Reference,
                 hospital_ref: Reference, sample_ref: Reference, value, counter: Counter):
        """
        A new ClinicalRecord instance, either built from existing data or from scratch.
        :param id_value: A string being the BETTER ID of the ExaminationRecord instance.
        :param examination_ref: An Examination instance being the Examination that record is referring to.
        :param subject_ref: A Patient instance being the patient on which the record has been measured.
        :param hospital_ref: A Hospital instance being the hospital in which the record has been measured.
        :param sample_ref: A Sample instance being the sample on which the record has been measured.
        :param value: A string/int/float/CodeableConcept being the value of what is examined in that record.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

        # set up the resource attributes
        self.value = value
        self.subject = subject_ref
        self.recorded_by = hospital_ref
        self.instantiate = examination_ref
        self.based_on = sample_ref

    def get_type(self) -> str:
        return TableNames.EXAMINATION_RECORD.value

    def to_json(self):
        if isinstance(self.value, CodeableConcept) or isinstance(self.value, Coding) or isinstance(self.value, Reference):
            # complex type, we need to expand it with .to_json()
            expanded_value = self.value.to_json()
        elif isinstance(self.value, datetime):
            log.debug("The datetime value in ExaminationRecord is %s", self.value)
            expanded_value = get_mongodb_date_from_datetime(current_datetime=self.value)
        else:
            # primitive type, no need to expand it
            expanded_value = self.value

        json_clinical_record = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "value": expanded_value,
            "subject": self.subject.to_json(),
            "recordedBy": self.recorded_by.to_json(),
            "instantiate": self.instantiate.to_json(),
            "basedOn": self.based_on.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }

        return json_clinical_record
