import os.path

from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.setup_logger import log
from utils.Counter import Counter
from utils.utils import get_datetime_from_str


class InputOutput(Resource):
    def __init__(self, id_value: str, file: str, type: CodeableConcept, date: str, counter: Counter):
        super().__init__(id_value=id_value, resource_type="InputOutput", counter=counter)

        if not os.path.exists(file):
            # TODO Nelly: check also the file extension?
            log.error(f"{file} is not a file path.")
            self.file = ""
        else:
            self.file = file
        self.type = type
        if get_datetime_from_str(str_value=date) is None:
            log.error(f"{date} is not a date")
            self.date = ""
        else:
            self.date = date
