from typing import Any

from datatypes.Identifier import Identifier
from enums.TableNames import TableNames
from entities.Record import Record
from database.Counter import Counter


class PhenotypicRecord(Record):
    def __init__(self, id_value: str, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, value: Any,
                 counter: Counter, hospital_name: str, dataset_name: str):
        """
        A new PhenotypicRecord instance.
        :param id_value: A string being the BETTER ID of the PhenotypicRecord instance.
        :param feature_id: A PhenotypicFeature instance being the PhenotypicFeature that record is referring to.
        :param patient_id: A Patient instance being the patient on which the record has been measured.
        :param hospital_id: A Hospital instance being the hospital in which the record has been measured.
        :param value: A string/int/float/CodeableConcept being the value of what is examined in that record.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, feature_id=feature_id, patient_id=patient_id, hospital_id=hospital_id,
                         value=value, resource_type=TableNames.PHENOTYPIC_RECORD,
                         counter=counter, hospital_name=hospital_name, dataset_name=dataset_name)
