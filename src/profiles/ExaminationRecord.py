from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.Reference import Reference
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime
from utils.setup_logger import log


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
        self._value = value
        self._subject = subject_ref
        self._recorded_by = hospital_ref
        self._instantiate = examination_ref
        self._based_on = sample_ref

    @classmethod
    def get_type(cls) -> str:
        return TableNames.EXAMINATION_RECORD.value

    def to_json(self) -> dict:
        if isinstance(self._value, CodeableConcept) or isinstance(self._value, Coding) or isinstance(self._value, Reference):
            # complex type, we need to expand it with .to_json()
            expanded_value = self._value.to_json()
        elif isinstance(self._value, datetime):
            log.debug(f"The datetime value in ExaminationRecord is {self._value}")
            expanded_value = get_mongodb_date_from_datetime(current_datetime=self._value)
        else:
            # primitive type, no need to expand it
            expanded_value = self._value

        examination_record_json = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "value": expanded_value,
            "subject": self._subject.to_json(),
            "recordedBy": self._recorded_by.to_json(),
            "instantiate": self._instantiate.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }

        if self._based_on is not None:
            # there are samples in this dataset (BUZZI)
            # we add the field "basedOn", otherwise we do not add it
            examination_record_json["basedOn"] = self._based_on.to_json()

        return examination_record_json
