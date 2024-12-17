from entities.Resource import Resource
from database.Counter import Counter


class ResourceTest(Resource):
    def __init__(self, id_value: int, entity_type: str, counter: Counter):
        super().__init__(id_value=id_value, entity_type=entity_type, counter=counter)
