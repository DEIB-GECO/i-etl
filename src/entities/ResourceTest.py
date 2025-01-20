from entities.Resource import Resource
from database.Counter import Counter


class ResourceTest(Resource):
    def __init__(self, identifier: int, counter: Counter):
        super().__init__(identifier=identifier, counter=counter)
