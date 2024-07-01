from profiles.Patient import Patient
from utils.TableNames import TableNames
from utils.Counter import Counter


class TestPatient:
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        counter = Counter()
        patient1 = Patient(id_value="123", counter=counter)
        assert patient1.identifier is not None
        assert patient1.identifier.value == TableNames.PATIENT.value + "/123"

        # TODO Nelly: check how to verify that a Patient cannot be created with a NON_VALUE id
        # I tried with self.assertRaises(ValueError, Patient(NONE_VALUE, TableNames.PATIENT.value))
        # but this still raises the Exception and does not pass the test

    def test_get_type(self):
        """
        Check whether the Patient type is indeed the Patient table name.
        :return: None.
        """
        counter = Counter()
        patient1 = Patient(id_value="123", counter=counter)
        assert patient1.get_type() == TableNames.PATIENT.value

    def test_to_json(self):
        counter = Counter()
        patient1 = Patient(id_value="123", counter=counter)
        patient1_json = patient1.to_json()

        assert patient1_json is not None
        assert "identifier" in patient1_json
        assert patient1_json["identifier"]["value"] == TableNames.PATIENT.value + "/123"
        assert "resourceType" in patient1_json
        assert patient1_json["resourceType"] == TableNames.PATIENT.value
