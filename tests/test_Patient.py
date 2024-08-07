from datatypes.PatientAnonymizedIdentifier import PatientAnonymizedIdentifier
from enums.HospitalNames import HospitalNames
from profiles.Patient import Patient
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.constants import DELIMITER_PATIENT_ID


class TestPatient:
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        counter = Counter()
        patient1 = Patient(id_value="123", counter=counter, hospital_name=HospitalNames.TEST_H1)
        assert patient1.identifier is not None
        assert patient1.identifier.value == PatientAnonymizedIdentifier(id_value="123", hospital_name=HospitalNames.TEST_H1).value

        # TODO Nelly: check that a Patient can be created with a NO_ID id
        # I tried with self.assertRaises(ValueError, Patient(NO_ID, TableNames.PATIENT))
        # but this still raises the Exception and does not pass the test

    def test_get_type(self):
        """
        Check whether the Patient type is indeed the Patient table name.
        :return: None.
        """
        counter = Counter()
        patient1 = Patient("123", counter=counter, hospital_name=HospitalNames.TEST_H1)
        assert patient1.resource_type == TableNames.PATIENT

    def test_to_json(self):
        counter = Counter()
        patient1 = Patient("123", counter=counter, hospital_name=HospitalNames.TEST_H1)
        patient1_json = patient1.to_json()

        assert patient1_json is not None
        assert patient1_json == {
            "identifier": {
                "value": PatientAnonymizedIdentifier(id_value="123", hospital_name=HospitalNames.TEST_H1).value
            },
            "resource_type": TableNames.PATIENT,
            "timestamp": patient1.timestamp
        }
