from typing import Any

from datatypes.Identifier import Identifier
from entities.Resource import Resource
from database.Counter import Counter
from utils.setup_logger import log


class Record(Resource):
    def __init__(self, id_value: str, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, value: Any, resource_type: str, counter: Counter,
                 hospital_name: str, dataset: str):
        """

        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.value = value  # original or anonymized value
        self.has_subject = patient_id.value  # Reference(resource_identifier=patient_id)
        self.registered_by = hospital_id.value  # Reference(resource_identifier=hospital_id)
        self.instantiates = feature_id.value  # Reference(resource_identifier=feature_id)
        self.dataset = dataset
