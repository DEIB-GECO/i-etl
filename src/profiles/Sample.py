from datetime import datetime

from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter
from src.utils.setup_logger import log
from src.utils.utils import get_mongodb_date_from_datetime, is_not_nan


class Sample(Resource):
    def __init__(self, id_value: str, quality: str, sampling: str, time_collected: datetime, time_received: datetime,
                 too_young: bool, bis: bool, counter: Counter):
        # set up the resource ID
        # this corresponds to the SampleBarcode in Buzzi data
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

        # set up the resource attributes
        self.sampling = sampling
        self.quality = quality
        self.time_collected = time_collected
        self.time_received = time_received
        self.too_young = too_young
        self.bis = bis

    def get_type(self):
        return TableNames.SAMPLE.value

    def to_json(self):
        json_sample = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "createdAt": get_mongodb_date_from_datetime(datetime.now())
        }

        # we need to check whether each field is a NaN value or not because we do not want to add fields for NaN values
        # we don't need to do it for ExaminationRecord instances because we create them only if the (single) value is not NaN
        if is_not_nan(self.quality):
            json_sample["quality"] = self.quality
        if is_not_nan(self.sampling):
            json_sample["sampling"] = self.sampling
        if is_not_nan(self.time_collected):
            json_sample["timeCollected"] = get_mongodb_date_from_datetime(self.time_collected)
        if is_not_nan(self.time_collected):
            json_sample["timeReceived"] = get_mongodb_date_from_datetime(self.time_collected)
        if is_not_nan(self.too_young):
            json_sample["tooYoung"] = self.too_young
        if is_not_nan(self.bis):
            json_sample["BIS"] = self.bis

        return json_sample
