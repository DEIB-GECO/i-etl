import os.path

from src.datatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.setup_logger import log
from utils.Counter import Counter
from utils.utils import get_datetime_from_str


class InputOutput(Resource):
    def __init__(self, id_value: str, file: str, type: CodeableConcept, date: str, counter: Counter):
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

        if not os.path.exists(file):
            # TODO Nelly: check also the file extension?
            log.error("%s is not a file path.", file)
            self.file = ""
        else:
            self.file = file
        self.type = type
        if get_datetime_from_str(str_value=date) is None:
            log.error("%s is not a date", date)
            self.date = ""
        else:
            self.date = date

    def get_type(self) -> str:
        # do not use a TableName here as we do not create a specific table for them,
        # instead we nest them (as JSON dicts) in Analysis input and output
        return "InputOutput"

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "file": self.file,
            "type": self.type,
            "date": self.date
        }
