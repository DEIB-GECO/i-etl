import json

from src.utils.setup_logger import log


class Reference:
    def __init__(self, resource):
        """
        Create a new reference to another resource.
        :param reference_url: the (unique) url assigned to the referred resource.
        :param type: the resource type associated to the referred resource, e.g., Patient, Organization, etc.
        """
        super().__init__()
        self.ref = resource.get_url()
        self.type = type(resource).__name__

    def to_json(self):
        json_reference = {
            "reference": str(self.ref),
            "type": str(self.type)
        }
        return json_reference

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
