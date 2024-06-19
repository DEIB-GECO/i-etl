from datetime import datetime

from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter
from src.utils.utils import get_mongodb_date_from_datetime


class Patient(Resource):
    def __init__(self, id_value: str, counter: Counter):
        """
        A new patient instance, either built from existing data or from scratch.
        :param id_value: A string being the ID of the patient assigned by the hospital.
        This ID is shared by the different patent sample, and SHOULD be shared by the hospitals.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

    def get_type(self) -> str:
        """
        Get the resource type, i.e., Patient.
        :return: A string being the resource type, i.e., Patient.
        """
        return TableNames.PATIENT.value

    def to_json(self) -> dict:
        """
        Get the JSON representation of the resource.
        :return: A JSON dict being the Patient with all its attributes.
        """
        json_patient = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }

        return json_patient
