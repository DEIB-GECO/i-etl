import json

from src.datatypes.Identifier import Identifier
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log
from src.utils import constants
from src.utils.Counter import Counter


class Resource:
    def __init__(self, id_value: str, resource_type: str, counter: Counter):
        """

        :param id_value:
        :param resource_type:
        """
        self.identifier = None  # change the FHIR model to have an identifier which is simply a string
        if id_value == constants.NONE_VALUE:
            if resource_type == TableNames.PATIENT.value or resource_type == TableNames.SAMPLE.value:
                # Patient instances should always have an ID (given by the hospitals)
                raise ValueError("Patient and Sample instances should have an ID.")
            else:
                # We assign an ID to the new resource
                self.identifier = Identifier(id_value=str(counter.increment()), resource_type=resource_type)
        else:
            # This case covers when we retrieve resources from the DB, and we reconstruct them in-memory:
            # they already have an identifier, thus we simply reconstruct it with the value
            self.identifier = Identifier(id_value=id_value, resource_type=resource_type)

        self.timestamp = None  # TODO Nelly: add insertedAt to the Resource class?

    def get_type(self):
        raise NotImplementedError("The method get_resource_type() has to be overridden in every child class.")

    def to_json(self):
        raise NotImplementedError("The method to_json() has to be overridden in every child class.")

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
