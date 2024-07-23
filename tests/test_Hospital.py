from profiles.Hospital import Hospital
from enums.TableNames import TableNames
from utils.constants import NO_ID
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class TestHospital:

    def test_constructor(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)

        assert hospital1 is not None
        assert hospital1.identifier is not None
        assert hospital1.identifier.value == "123"

        hospital2 = Hospital(id_value=NO_ID, name="MyHospital", counter=counter)
        assert hospital2 is not None
        assert hospital2.identifier is not None
        assert hospital2.identifier.value == "1"

    def test_get_type(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        assert hospital1 is not None
        assert hospital1.resource_type == TableNames.HOSPITAL

        hospital2 = Hospital(id_value=NO_ID, name="MyHospital", counter=counter)
        assert hospital2 is not None
        assert hospital2.resource_type == TableNames.HOSPITAL

    def test_to_json(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        hospital1_json = hospital1.to_json()

        assert hospital1_json is not None
        assert hospital1_json == {
            "identifier": {"value": "123"},
            "resource_type": TableNames.HOSPITAL,
            "name": "MyHospital",
            "timestamp": hospital1.timestamp}
