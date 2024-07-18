import json

from datatypes.Identifier import Identifier
from utils.TableNames import TableNames
from utils import constants
from utils.Counter import Counter


class Resource:
    def __init__(self, id_value: int, resource_type: str, counter: Counter):
        """

        :param id_value:
        :param resource_type:
        """
        self._identifier = None  # change the FHIR model to have an identifier which is simply a string
        if id_value == constants.NO_ID:
            if resource_type == TableNames.PATIENT.value or resource_type == TableNames.SAMPLE.value:
                # Patient instances should always have an ID (given by the hospitals)
                raise ValueError("Patient and Sample instances should have an ID.")
            else:
                # We assign an ID to the new resource
                self._identifier = Identifier(id_value=counter.increment(), resource_type=resource_type)
        else:
            # This case covers when we retrieve resources from the DB, and we reconstruct them in-memory:
            # they already have an identifier, thus we simply reconstruct it with the value
            self._identifier = Identifier(id_value=id_value, resource_type=resource_type)

        self._timestamp = None  # TODO Nelly: add insertedAt to the Resource class?

    @property
    def identifier(self) -> Identifier:
        return self._identifier

    @classmethod
    def get_type(cls):
        raise NotImplementedError("The method get_resource_type() has to be overridden in every child class.")

    def to_json(self):
        raise NotImplementedError("The method to_json() has to be overridden in every child class.")

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def __repr__(self) -> str:
        return json.dumps(self.to_json())
