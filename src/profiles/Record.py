from typing import Any

from datatypes.Identifier import Identifier
from datatypes.Reference import Reference
from profiles.Resource import Resource
from utils.Counter import Counter
from utils.setup_logger import log


class Record(Resource):
    def __init__(self, id_value: str, feature_id: Identifier, patient_id: Identifier,
                 hospital_id: Identifier, value: Any, resource_type: str, counter: Counter, hospital_name: str):
        """

        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.value = value
        self.subject = Reference(resource_identifier=patient_id)
        self.recorded_by = Reference(resource_identifier=hospital_id)
        self.instantiate = Reference(resource_identifier=feature_id)
