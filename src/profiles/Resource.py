import json

from src.fhirDatatypes.Identifier import Identifier
from src.utils.IdUsages import IdUsages
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE
from src.utils.setup_logger import log


class Resource:
    ID_COUNTER = 1

    def __init__(self, id_value: str, resource_type: str):
        """

        :param id_value:
        :param resource_type:
        """
        self.identifier = None
        if id_value == NONE_VALUE:
            if resource_type == TableNames.PATIENT.value:
                # Patient instances should always have an ID (given by the hospitals)
                raise ValueError("A Patient instance should have an ID.")
            else:
                # We assign an ID to the new resource
                self.identifier = Identifier(id_value=str(Resource.ID_COUNTER), resource_type=resource_type, use=IdUsages.ASSIGNED_BY_BETTER.value)
                Resource.ID_COUNTER = Resource.ID_COUNTER + 1
        else:
            # If the id_value is not None, this means that:
            # (a) we are either building/retrieving a Patient/Sample (having, in any case an ID provided by the hospitals)
            # (b) or we are retrieving an existing resource (which is not a Patient)
            if resource_type == TableNames.PATIENT.value:
                # (a) we create a new Patient instance, having an ID provided by the hospital (absolute need to reuse them)
                # or we are retrieving an existing Patient
                self.identifier = Identifier(id_value=id_value, resource_type=resource_type, use=IdUsages.ASSIGNED_BY_HOSPITAL.value)
            elif resource_type == TableNames.SAMPLE.value:
                # (a) we create a new Sample instance, having an ID provided by the hospital (absolute need to reuse them)
                # or we are retrieving an existing Sample
                self.identifier = Identifier(id_value=id_value, resource_type=resource_type, use=IdUsages.ASSIGNED_BY_HOSPITAL.value)
            else:
                # (b) we build a new in-memory Resource by filling the attributes with what is already in the database
                self.identifier = Identifier(id_value=id_value, resource_type=resource_type, use=IdUsages.ASSIGNED_BY_BETTER.value)

    def get_type(self):
        raise NotImplementedError("The method get_resource_type() has to be overridden in every child class.")

    def to_json(self):
        raise NotImplementedError("The method to_json() has to be overridden in every child class.")

    def from_json(self, the_json: dict):
        raise NotImplementedError("The method from_json() has to be overridden in every child class.")

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
