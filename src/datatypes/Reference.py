import jsonpickle

from datatypes.Identifier import Identifier


class Reference:
    """
    The class Reference implements the FHIR Reference data type.
    This allows to refer to other resources using their BETTER ID
    (not the local_id, which is proper to each hospital, but instead the id
    """
    def __init__(self, resource_identifier: Identifier):
        """
        Create a new reference to another resource.
        :param resource_identifier:
        """
        self.reference = resource_identifier.value

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
