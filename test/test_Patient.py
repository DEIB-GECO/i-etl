import unittest

from src.profiles.Patient import Patient
from src.utils.TableNames import TableNames


class TestPatient(unittest.TestCase):
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        patient1 = Patient("123")
        self.assertIsNotNone(patient1.identifier)
        self.assertEqual(patient1.identifier, TableNames.PATIENT.value + "/123")

        # TODO Nelly: check how to verify that a Patient cannot be created with a NON_VALUE id
        # I tried with self.assertRaises(ValueError, Patient(NONE_VALUE, TableNames.PATIENT.value))
        # but this still raises the Exception and does not pass the test

    def test_get_type(self):
        """
        Check whether the Patient type is indeed the Patient table name.
        :return: None.
        """
        patient1 = Patient("123")
        self.assertEqual(patient1.get_type(), TableNames.PATIENT.value)

    def test_to_json(self):
        patient1 = Patient("123")
        patient1_json = patient1.to_json()

        self.assertIsNotNone(patient1_json)
        self.assertIn("identifier", patient1_json)
        self.assertEqual(patient1_json["identifier"], TableNames.PATIENT.value + "/123")
        self.assertIn("resourceType", patient1_json)
        self.assertEqual(patient1_json["resourceType"], TableNames.PATIENT.value)

    def test_from_json(self):
        json_patient = {
            "identifier": TableNames.PATIENT.value + "/123",
            "resourceType": TableNames.PATIENT.value
        }

        patient = Patient.from_json(json_patient)
        self.assertIsNotNone(patient.identifier)
        self.assertEqual(patient.identifier, TableNames.PATIENT.value + "/123")
        self.assertEqual(patient.get_type(), TableNames.PATIENT.value)


if __name__ == '__main__':
    unittest.main()
