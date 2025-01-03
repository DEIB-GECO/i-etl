from typing import Any

from database.Counter import Counter
from datatypes.Identifier import Identifier
from entities.Record import Record
from enums.Profile import Profile


class PhenotypicRecord(Record):
    def __init__(self, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, value: Any,
                 counter: Counter, dataset: str):
        """
        A new PhenotypicRecord instance.
        :param feature_id: A PhenotypicFeature instance being the PhenotypicFeature that record is referring to.
        :param patient_id: A Patient instance being the patient on which the record has been measured.
        :param hospital_id: A Hospital instance being the hospital in which the record has been measured.
        :param value: A string/int/float/CodeableConcept being the value of what is examined in that record.
        """
        # set up the resource ID
        super().__init__(feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         value=value, profile=Profile.PHENOTYPIC, counter=counter, dataset=dataset)
