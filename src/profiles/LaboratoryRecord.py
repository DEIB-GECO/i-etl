from typing import Any

from datatypes.Reference import Reference
from enums.TableNames import TableNames
from profiles.Record import Record
from utils.Counter import Counter


class LaboratoryRecord(Record):
    def __init__(self, id_value: str, examination_ref: Reference, subject_ref: Reference,
                 hospital_ref: Reference, sample_ref: Reference, value: Any, counter: Counter):
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
        super().__init__(id_value=id_value, feature_ref=examination_ref, subject_ref=subject_ref, hospital_ref=hospital_ref,
                         value=value, resource_type=TableNames.LABORATORY_RECORD, counter=counter)
        # set up specific attributes for lab records
        self.based_on = sample_ref
