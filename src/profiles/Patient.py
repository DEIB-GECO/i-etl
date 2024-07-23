from datetime import datetime

from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class Patient(Resource):
    def __init__(self, id_value: str, counter: Counter):
        """
        A new patient instance, either built from existing data or from scratch.
        :param id_value: A string being the ID of the patient assigned by the hospital.
        This ID is shared by the different patent sample, and SHOULD be shared by the hospitals.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=TableNames.PATIENT, counter=counter)
