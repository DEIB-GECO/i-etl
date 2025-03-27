from constants.defaults import NO_ID
from database.Counter import Counter
from entities.Feature import Feature
from entities.Hospital import Hospital
from entities.Resource import Resource
from enums.HospitalNames import HospitalNames
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
            Resource.IDENTIFIER_: 1,
            Feature.NAME_: HospitalNames.TEST_H1,
            Resource.TIMESTAMP_: hospital1.timestamp,
            Resource.ENTITY_TYPE_: TableNames.HOSPITAL
        }
