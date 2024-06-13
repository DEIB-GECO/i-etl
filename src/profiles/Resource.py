import json

from src.utils.TableNames import TableNames
from src.utils.utils import create_identifier
from src.utils.constants import NONE_VALUE


class Resource:
    ID_COUNTER = 1

    def __init__(self, id_value: str, resource_type: str):
        """

        :param id_value:
        :param resource_type:
        """
        self.identifier = None  # change the FHIR model to have an identifier which is simply a string
        if id_value == NONE_VALUE:
            if resource_type == TableNames.PATIENT.value or resource_type == TableNames.SAMPLE.value:
                # Patient instances should always have an ID (given by the hospitals)
                raise ValueError("Patient and Sample instances should have an ID.")
            else:
                # We assign an ID to the new resource
                self.identifier = create_identifier(id_value=str(Resource.ID_COUNTER), resource_type=resource_type)
                Resource.ID_COUNTER = Resource.ID_COUNTER + 1
        else:
            # TODO Nelly: explain this case
            self.identifier = create_identifier(id_value=id_value, resource_type=resource_type)

        self.timestamp = None  # TODO Nelly: add insertedAt to the Resource class?

    def get_type(self):
        raise NotImplementedError("The method get_resource_type() has to be overridden in every child class.")

    def to_json(self):
        raise NotImplementedError("The method to_json() has to be overridden in every child class.")

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
