from datetime import datetime

from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter
from src.utils.setup_logger import log
from src.utils.utils import get_mongodb_date_from_datetime


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
            "quality": self.quality,
            "sampling": self.sampling,
            "timeCollected": get_mongodb_date_from_datetime(self.time_collected),
            "timeReceived": get_mongodb_date_from_datetime(self.time_collected),
            "tooYoung": self.too_young,
            "BIS": self.bis,
            "createdAt": get_mongodb_date_from_datetime(datetime.now())
        }
        return json_sample
