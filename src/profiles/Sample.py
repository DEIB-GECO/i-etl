from datetime import datetime

from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime, is_not_nan


class Sample(Resource):
    def __init__(self, id_value: str, quality: str, sampling: str, time_collected: datetime, time_received: datetime,
                 too_young: bool, bis: bool, counter: Counter):
        # set up the resource ID
        # this corresponds to the SampleBarcode in Buzzi data
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

        # set up the resource attributes
        self._sampling = sampling
        self._quality = quality
        self._time_collected = time_collected
        self._time_received = time_received
        self._too_young = too_young
        self._bis = bis

    @classmethod
    def get_type(cls) -> str:
        return TableNames.SAMPLE.value

    def to_json(self) -> dict:
        json_sample = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }

        # we need to check whether each field is a NaN value or not because we do not want to add fields for NaN values
        # we don't need to do it for ExaminationRecord instances because we create them only if the (single) value is not NaN
        if is_not_nan(self._quality):
            json_sample["quality"] = self._quality
        if is_not_nan(self._sampling):
            json_sample["sampling"] = self._sampling
        if is_not_nan(self._time_collected):
            json_sample["timeCollected"] = get_mongodb_date_from_datetime(current_datetime=self._time_collected)
        if is_not_nan(self._time_collected):
            json_sample["timeReceived"] = get_mongodb_date_from_datetime(current_datetime=self._time_collected)
        if is_not_nan(self._too_young):
            json_sample["tooYoung"] = self._too_young
        if is_not_nan(self._bis):
            json_sample["BIS"] = self._bis

        return json_sample
