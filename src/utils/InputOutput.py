from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.setup_logger import log


class InputOutput(Resource):
    def __init__(self, id_value: str, file: str, type: CodeableConcept, date: str):
        super().__init__(id_value)

        if not is_filepath(file):
            # TODO Nelly: check also the file extension?
            log.error("%s is not a file path.", file)
            self.file = ""
        else:
            self.file = file
        self.type = type
        if not is_date(date):
            log.error("%s is not a date", date)
            self.date = ""
        else:
            self.date = date

    def get_type(self):
        # do not use a TableName here as we do not create a specific table for them,
        # instead we nest them (as JSON dicts) in Analysis input and output
        return "InputOutput"

    def to_json(self):
        return {
            "identifier": self.identifier,
            "resourceType": self.get_type(),
            "file": self.file,
            "type": self.type,
            "date": self.date
        }
