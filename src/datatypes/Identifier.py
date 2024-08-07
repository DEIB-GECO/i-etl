import jsonpickle

from enums.HospitalNames import HospitalNames
from utils.constants import DELIMITER_PATIENT_ID


class Identifier:
    def __init__(self, value: str):
        self.value = value

    def get_as_int(self):
        raise NotImplemented("This method should be overridden in child classes.")

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
