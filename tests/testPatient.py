import unittest

from src.profiles.Patient import Patient
from src.utils.TableNames import TableNames


class TestPatient(unittest.TestCase):
    NB_TESTS = 3
    NB_TESTS_RUN = 0

    def run_all(self):
        self.test_constructor()
        self.test_get_type()
        self.test_to_json()

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

        TestPatient.NB_TESTS_RUN = TestPatient.NB_TESTS_RUN + 1

    def test_get_type(self):
        """
        Check whether the Patient type is indeed the Patient table name.
        :return: None.
        """
        patient1 = Patient("123")
        self.assertEqual(patient1.get_type(), TableNames.PATIENT.value)

        TestPatient.NB_TESTS_RUN = TestPatient.NB_TESTS_RUN + 1

    def test_to_json(self):
        patient1 = Patient("123")
        patient1_json = patient1.to_json()

        self.assertIsNotNone(patient1_json)
        self.assertIn("identifier", patient1_json)
        self.assertEqual(patient1_json["identifier"], TableNames.PATIENT.value + "/123")
        self.assertIn("resourceType", patient1_json)
        self.assertEqual(patient1_json["resourceType"], TableNames.PATIENT.value)

        TestPatient.NB_TESTS_RUN = TestPatient.NB_TESTS_RUN + 1
