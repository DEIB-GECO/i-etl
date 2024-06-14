import unittest

from src.profiles.Patient import Patient
from src.utils.TableNames import TableNames
from src.utils.Counter import Counter


class TestPatient(unittest.TestCase):
    def test_constructor(self):
        """
        Test whether the Patient constructor correctly assign IDs and the resource type.
        :return: None.
        """
        counter = Counter()
        patient1 = Patient(id_value="123", counter=counter)
        self.assertIsNotNone(patient1.identifier)
        self.assertEqual(patient1.identifier.value, TableNames.PATIENT.value + "/123")

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
        self.assertEqual(patient1.get_type(), TableNames.PATIENT.value)

    def test_to_json(self):
        counter = Counter()
        patient1 = Patient(id_value="123", counter=counter)
        patient1_json = patient1.to_json()

        self.assertIsNotNone(patient1_json)
        self.assertIn("identifier", patient1_json)
        self.assertEqual(patient1_json["identifier"]["value"], TableNames.PATIENT.value + "/123")
        self.assertIn("resourceType", patient1_json)
        self.assertEqual(patient1_json["resourceType"], TableNames.PATIENT.value)
