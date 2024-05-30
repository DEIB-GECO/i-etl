import json

from src.utils.setup_logger import log


class Identifier:
    def __init__(self, id_value: str, resource_type: str, use: str):
        self.value = self.create_identifier(id_value, resource_type)
        self.use = use  # corresponding enum: IdUsages; 'official' for IDs given by hospitals, 'secondary' for the BETTER IDs

    def create_identifier(self, id_value: str, resource_type: str) -> str:
        if "/" in id_value:
            # the provided id_value is already of the form type/id, thus we do not need to append the resource type
            # this happens when we build (instance) resources from the existing data in the database
            # the stored if is already of the form type/id
            return id_value
        else:
            return resource_type + "/" + id_value

    def to_json(self):
        json_identifier = {
            "value": self.value,
            "use": self.use,
        }
        return json_identifier

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())

    def __eq__(self, other) -> bool:
        if isinstance(other, Identifier):
            return self.value == other.value
        return False

    def __hash__(self):
        return hash(self.value)
