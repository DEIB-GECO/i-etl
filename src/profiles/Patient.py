import json

from src.database.TableNames import TableNames
from src.profiles.Resource import Resource
from src.utils.setup_logger import log


class Patient(Resource):
    ID_COUNTER = 1

    def __init__(self, csv_line, csv_filpath):
        super().__init__()
        self.id = Patient.ID_COUNTER
        Patient.ID_COUNTER = Patient.ID_COUNTER + 1
        self.csv_line = csv_line
        self.csv_filepath = csv_filpath

    def get_id(self):
        return self.id

    def get_resource_type(self):
        return TableNames.PATIENT_TABLE_NAME

    def to_json(self):
        patient = {
            "id": self.id,
            "metadata": {
                "csv_filepath": self.csv_filepath,
                "csv_line": self.csv_line
            }
        }

        return json.dumps(patient)

    def __str__(self):
        log.debug("str for Patient: %s", self.to_json())
        return self.to_json()

    def __repr__(self):
        log.debug("str for Patient: %s", self.to_json())
        return self.to_json()
