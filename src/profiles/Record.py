from typing import Any

from datatypes.Reference import Reference
from profiles.Resource import Resource
from utils.Counter import Counter


class Record(Resource):
    def __init__(self, id_value: str, feature_ref: Reference, subject_ref: Reference,
                 hospital_ref: Reference, value: Any, resource_type: str, counter: Counter):
        """

        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter)

        # set up the resource attributes
        self.value = value
        self.subject = subject_ref
        self.recorded_by = hospital_ref
        self.instantiate = feature_ref
