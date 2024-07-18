from profiles.Resource import Resource
from utils.Counter import Counter
from utils.TableNames import TableNames


class ResourceTest(Resource):
    def __init__(self, id_value: int, resource_type: str, counter: Counter):
        super().__init__(id_value, resource_type, counter)

    @classmethod
    def get_type(cls) -> str:
        return TableNames.TEST.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type()
        }