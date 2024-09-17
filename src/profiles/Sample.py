from datetime import datetime

from enums.TableNames import TableNames
from profiles.Resource import Resource
from database.Counter import Counter


class Sample(Resource):
    def __init__(self, id_value: str, quality: str, sampling: str, time_collected: datetime, time_received: datetime,
                 too_young: bool, bis: bool, counter: Counter, hospital_name: str):
        # set up the resource ID
        # this corresponds to the SampleBarcode in Buzzi data
        super().__init__(id_value=id_value, resource_type=TableNames.SAMPLE, counter=counter, hospital_name=hospital_name)

        # set up the resource attributes
        self.sampling = sampling
        self.quality = quality
        self.time_collected = time_collected
        self.time_received = time_received
        self.too_young = too_young
        self.bis = bis
