from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames


class Sample(Resource):
    def __init__(self, id_value):
        super().__init__(id_value=id_value, resource_type=self.get_type())

    def get_type(self):
        return TableNames.SAMPLE.value

    def to_json(self):
        json_sample = {
            "identifier": self.identifier,
            "resourceType": self.get_type(),
        }
        return json_sample
