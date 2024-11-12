from datatypes.PatientAnonymizedIdentifier import PatientAnonymizedIdentifier
from enums.HospitalNames import HospitalNames
from entities.Patient import Patient
from enums.TableNames import TableNames
from database.Counter import Counter
from constants.idColumns import NO_ID
from utils.setup_logger import log


class TestPatient:
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        # this is a new Patient, thus with a new anonymised ID
        counter = Counter()
        patient1 = Patient(id_value=NO_ID, counter=counter, hospital_name=HospitalNames.TEST_H1)
        anonymised_p1 = PatientAnonymizedIdentifier(id_value="1", hospital_name=HospitalNames.TEST_H1).value
        assert patient1.identifier is not None
        assert patient1.identifier.value == anonymised_p1

        # this is an existing Patient, for which an anonymized ID already exists
        counter = Counter()
        patient1 = Patient(id_value="h1:123", counter=counter, hospital_name=HospitalNames.TEST_H1)
        anonymized_p1 = PatientAnonymizedIdentifier(id_value="h1:123", hospital_name=None).value
        assert patient1.identifier is not None
        assert patient1.identifier.value == anonymized_p1

    def test_to_json(self):
        counter = Counter()
        patient1 = Patient(NO_ID, counter=counter, hospital_name=HospitalNames.TEST_H1)
        patient1_json = patient1.to_json()

        assert patient1_json is not None
        assert patient1_json == {
            "identifier": PatientAnonymizedIdentifier(id_value="1", hospital_name=HospitalNames.TEST_H1).to_json(),
            "timestamp": patient1.timestamp
        }
