from enums.TableNames import TableNames
from profiles.Resource import Resource
from database.Counter import Counter


class Patient(Resource):
    def __init__(self, id_value: str, counter: Counter, hospital_name: str):
        """
        A new patient instance, either built from existing data or from scratch.
        :param id_value: A string being the ID of the patient assigned by the hospital.
        This ID is shared by the different patent sample, and SHOULD be shared by the hospitals.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=TableNames.PATIENT, counter=counter, hospital_name=hospital_name)
