from constants.defaults import NO_ID
from database.Counter import Counter
from entities.Hospital import Hospital
from enums.HospitalNames import HospitalNames
from enums.ResourceKeys import ResourceKeys
from enums.TableNames import TableNames


class TestHospital:

    def test_constructor(self):
        counter = Counter()
        hospital1 = Hospital(identifier=NO_ID, name=HospitalNames.TEST_H1, counter=counter)

        assert hospital1 is not None
        assert hospital1.identifier is not None
        assert hospital1.identifier == 1

    def test_to_json(self):
        counter = Counter()
        hospital1 = Hospital(identifier=NO_ID, name=HospitalNames.TEST_H1, counter=counter)
        hospital1_json = hospital1.to_json()

        assert hospital1_json is not None
        assert hospital1_json == {
            ResourceKeys.FEATURE_IDENTIFIER: 1,
            ResourceKeys.F_NAME: HospitalNames.TEST_H1,
            ResourceKeys.FEATURE_TIMESTAMP: hospital1.timestamp,
            ResourceKeys.FEATURE_ENTITY: TableNames.HOSPITAL
        }
