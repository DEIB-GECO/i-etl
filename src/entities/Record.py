from typing import Any

from datatypes.Identifier import Identifier
from datatypes.Reference import Reference
from entities.Resource import Resource
from database.Counter import Counter


class Record(Resource):
    def __init__(self, id_value: str, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, value: Any, anonymized_value: Any, resource_type: str, counter: Counter,
                 hospital_name: str, dataset_name: str):
        """

        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.value = value
        if anonymized_value is not None:
            self.anonymized_value = anonymized_value
        self.subject = patient_id.value  # Reference(resource_identifier=patient_id)
        self.recorded_by = hospital_id.value  # Reference(resource_identifier=hospital_id)
        self.instantiate = feature_id.value  # Reference(resource_identifier=feature_id)
        self.dataset_name = dataset_name
