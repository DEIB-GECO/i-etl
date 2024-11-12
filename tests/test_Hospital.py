from datatypes.ResourceIdentifier import ResourceIdentifier
from enums.HospitalNames import HospitalNames
from entities.Hospital import Hospital
from enums.TableNames import TableNames
from constants.idColumns import NO_ID
from database.Counter import Counter


class TestHospital:

    def test_constructor(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name=HospitalNames.TEST_H1, counter=counter)

        assert hospital1 is not None
        assert hospital1.identifier is not None
        assert hospital1.identifier.value == ResourceIdentifier(id_value="123", resource_type=TableNames.HOSPITAL).value

        hospital2 = Hospital(id_value=NO_ID, name=HospitalNames.TEST_H1, counter=counter)
        assert hospital2 is not None
        assert hospital2.identifier is not None
        assert hospital2.identifier.value == ResourceIdentifier(id_value="1", resource_type=TableNames.HOSPITAL).value

    def test_to_json(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name=HospitalNames.TEST_H1, counter=counter)
        hospital1_json = hospital1.to_json()

        assert hospital1_json is not None
        assert hospital1_json == {
            "identifier": "Hospital:123",
            "name": HospitalNames.TEST_H1,
            "timestamp": hospital1.timestamp
        }
