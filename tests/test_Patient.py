from constants.defaults import NO_ID
from database.Counter import Counter
from datatypes.Identifier import Identifier
from entities.Patient import Patient
from enums.HospitalNames import HospitalNames
from enums.TableNames import TableNames


class TestPatient:
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        # this is a new Patient, thus with a new anonymised ID
        counter = Counter()
        patient1 = Patient(id_value=NO_ID, counter=counter, hospital_name=HospitalNames.TEST_H1)
        assert patient1.identifier is not None
        assert patient1.identifier == 1

        # this is an existing Patient, for which an anonymized ID already exists
        counter = Counter()
        patient1 = Patient(id_value=123, counter=counter, hospital_name=HospitalNames.TEST_H1)
        assert patient1.identifier is not None
        assert patient1.identifier == 123

    def test_to_json(self):
        counter = Counter()
        patient1 = Patient(id_value=NO_ID, counter=counter, hospital_name=HospitalNames.TEST_H1)
        patient1_json = patient1.to_json()

        assert patient1_json is not None
        assert patient1_json == {
            "identifier": 1,
            "timestamp": patient1.timestamp,
            "entity_type": TableNames.PATIENT
        }
