import json


class Reference:
    """
    The class Reference implements the FHIR Reference data type.
    This allows to refer to other resources using their BETTER ID
    (not the local_id, which is proper to each hospital, but instead the id
    """
    def __init__(self, resource):
        """
        Create a new reference to another resource.
        :param resource:
        """
        self.reference = resource.identifier
        self.type = type(resource).name__

    def to_json(self):
        json_reference = {
            "reference": self.reference.to_json(),
            "type": self.type
        }

        return json_reference

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
