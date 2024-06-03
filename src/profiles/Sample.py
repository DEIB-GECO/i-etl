from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames


class Sample(Resource):
    def __init__(self, id_value: str, quality: str, sampling: str, time_collected: str, time_received: str, too_young: bool, bis: bool):
        # set up the resource ID
        # this corresponds to the SampleBarcode in Buzzi data
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.quality = quality
        self.sampling = sampling
        self.time_collected = time_collected
        self.time_received = time_received
        self.too_young = too_young
        self.bis = bis

    def get_type(self):
        return TableNames.SAMPLE.value

    def to_json(self):
        json_sample = {
            "identifier": self.identifier,
            "resourceType": self.get_type(),
            "quality": self.quality,
            "sampling": self.sampling,
            "collected_at": self.time_collected,
            "received_at": self.time_collected,
            "too_young": self.too_young,
            "bis": self.bis
        }
        return json_sample
