import json

from src.database.TableNames import TableNames
from src.profiles.Resource import Resource
from src.utils.setup_logger import log


class Patient(Resource):
    ID_COUNTER = 1

    def __init__(self, csv_line, csv_filepath):
        super().__init__()
        self.id = Patient.ID_COUNTER
        Patient.ID_COUNTER = Patient.ID_COUNTER + 1
        self.url = "Patient" + str(self.id)
        self.csv_line = csv_line
        self.csv_filepath = csv_filepath

    def get_id(self):
        return self.id

    def get_url(self):
        return self.url

    def get_resource_type(self):
        return TableNames.PATIENT_TABLE_NAME

    def to_json(self):
        json_patient = {
            "id": str(self.id),
            "metadata": {
                "csv_filepath": str(self.csv_filepath),
                "csv_line": str(self.csv_line)
            }
        }

        return json_patient
