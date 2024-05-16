import json

from src.utils.setup_logger import log


class Reference:
    def __init__(self, resource):
        """
        Create a new reference to another resource.
        :param resource:
        """
        super().__init__()
        self.ref = resource.get_url()
        self.type = type(resource).__name__

    def to_json(self):
        json_reference = {
            "reference": self.ref,
            "type": self.type
        }

        return json_reference

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
