from typing import Any

from datatypes.Identifier import Identifier
from datatypes.Reference import Reference
from enums.TableNames import TableNames
from profiles.Record import Record
from database.Counter import Counter


class LaboratoryRecord(Record):
    def __init__(self, id_value: str, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, sample_id: Identifier, value: Any, anonymized_value: Any,
                 counter: Counter, hospital_name: str):
        """
        A new LabRecord instance.
        :param id_value: A string being the BETTER ID of the LabRecord instance.
        :param feature_ref: An LabFeature instance being the LabFeature that record is referring to.
        :param patient_ref: A Patient instance being the patient on which the record has been measured.
        :param hospital_ref: A Hospital instance being the hospital in which the record has been measured.
        :param sample_ref: A Sample instance being the sample on which the record has been measured.
        :param value: A string/int/float/CodeableConcept being the value of what is examined in that record.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         value=value, anonymized_value=anonymized_value, resource_type=TableNames.LABORATORY_RECORD,
                         counter=counter, hospital_name=hospital_name)
        # set up specific attributes for lab records
        if sample_id is not None:
            self.based_on = Reference(resource_identifier=sample_id)
        else:
            self.based_on = None
