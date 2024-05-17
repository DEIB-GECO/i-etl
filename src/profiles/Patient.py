from src.profiles.Resource import Resource
from src.utils.utils import build_url, PATIENT_TABLE_NAME


class Patient(Resource):
    ID_COUNTER = 1

    def __init__(self, csv_line, csv_filepath):
        super().__init__()
        self.id = Patient.ID_COUNTER
        Patient.ID_COUNTER = Patient.ID_COUNTER + 1
        self.url = build_url(PATIENT_TABLE_NAME, self.id)
        self.csv_line = csv_line
        self.csv_filepath = csv_filepath

    def get_url(self) -> str:
        return self.url

    def get_resource_type(self) -> str:
        return PATIENT_TABLE_NAME

    def to_json(self) -> dict:
        json_patient = {
            "id": self.id,
            "url": self.url,
            "metadata": {
                "csv_filepath": self.csv_filepath,
                "csv_line": self.csv_line
            }
        }

        return json_patient
