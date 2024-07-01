from profiles.Hospital import Hospital
from utils.TableNames import TableNames
from utils.constants import NONE_VALUE
from utils.Counter import Counter


class TestHospital:
    def test_constructor(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)

        assert hospital1 is not None
        assert hospital1.identifier is not None
        assert hospital1.identifier.value == TableNames.HOSPITAL.value + "/123"

        hospital2 = Hospital(id_value=NONE_VALUE, name="MyHospital", counter=counter)
        assert hospital2 is not None
        assert hospital2.identifier is not None
        assert hospital2.identifier.value == TableNames.HOSPITAL.value + "/1"

    def test_get_type(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        assert hospital1 is not None
        assert hospital1.get_type() == TableNames.HOSPITAL.value

        hospital2 = Hospital(id_value=NONE_VALUE, name="MyHospital", counter=counter)
        assert hospital2  is not None
        assert hospital2.get_type() == TableNames.HOSPITAL.value

    def test_to_json(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        hospital1_json = hospital1.to_json()

        assert hospital1_json is not None
        assert "identifier" in hospital1_json
        assert hospital1_json["identifier"]["value"] == TableNames.HOSPITAL.value + "/123"
        assert "resourceType" in hospital1_json
        assert hospital1_json["resourceType"] == TableNames.HOSPITAL.value
