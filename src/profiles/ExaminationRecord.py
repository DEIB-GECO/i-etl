from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.Reference import Reference
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime, is_not_nan
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
        super().__init__(id_value=id_value, resource_type=TableNames.EXAMINATION_RECORD.value, counter=counter)

        # set up the resource attributes
        self.value = value
        self.subject = subject_ref
        self.recorded_by = hospital_ref
        self.instantiate = examination_ref
        self.based_on = sample_ref
