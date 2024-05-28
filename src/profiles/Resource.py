import json

from src.fhirDatatypes.Identifier import Identifier
from src.utils.IdUsages import IdUsages
from src.utils.TableNames import TableNames
from src.utils.utils import NONE_VALUE


class Resource:
    ID_COUNTER = 1

    def __init__(self, id_value, resource_type):
        self.identifier = None
        if id_value == NONE_VALUE:
            new_id = self.__create_identifier(str(Resource.ID_COUNTER), resource_type)
            self.identifier = Identifier(id_value=new_id, use=IdUsages.ASSIGNED_BY_BETTER.value)
            Resource.ID_COUNTER = Resource.ID_COUNTER + 1
        else:
            # This is:
            # (a) we create a new Patient instance, having an ID provided by the hospital (absolute need to reuse them)
            # (b) we build a new in-memory Resource by filling the attributes with what is already in the database
            existing_id = self.__create_identifier(id_value=id_value, resource_type=TableNames.PATIENT.value)
            self.identifier = Identifier(id_value=existing_id, use=IdUsages.ASSIGNED_BY_HOSPITAL.value)

    def __create_identifier(self, id_value: str, resource_type: str) -> str:
        if "/" in id_value:
            # the provided id_value is already of the form type/id, thus we do not need to append the resource type
            # this happens when we build (instance) resources from the existing data in the database
            # the stored if is already of the form type/id
            return id_value
        else:
            return resource_type + "/" + id_value

    def get_type(self):
        raise NotImplementedError("The method get_resource_type() has to be overridden in every child class.")

    def to_json(self):
        raise NotImplementedError("The method to_json() has to be overridden in every child class.")

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
