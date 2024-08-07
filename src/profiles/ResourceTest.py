from profiles.Resource import Resource
from utils.Counter import Counter
from enums.TableNames import TableNames


class ResourceTest(Resource):
    def __init__(self, id_value: str, resource_type: str, counter: Counter, hospital_name: str):
        super().__init__(id_value=id_value, resource_type=resource_type, counter=counter, hospital_name=hospital_name)
