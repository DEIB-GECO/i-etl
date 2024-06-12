import json


class BetterReference:
    """
    The class Reference implements the FHIR Reference data type.
    This allows to refer to other resources using their BETTER ID
    (not the local_id, which is proper to each hospital, but instead the id
    """
    def __init__(self, resource_identifier: str, resource_type: str):
        """
        Create a new reference to another resource.
        :param resource_identifier:
        """
        self.reference = resource_identifier
        self.type = resource_type

    def to_json(self):
        json_reference = {
            "reference": self.reference,
            "type": self.type
        }

        return json_reference

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
