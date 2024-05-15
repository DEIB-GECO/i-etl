from src.database.TableNames import TableNames
from src.profiles.Resource import Resource


class DiseaseRecord(Resource):
    def __init__(self):
        super().__init__()
        pass

    def get_resource_type(self):
        return TableNames.DISEASE_RECORD_TABLE_NAME

    def to_json(self):
        return {}
