import unittest

from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.utils.IdUsages import IdUsages
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE


class TestHospital(unittest.TestCase):
    def test_constructor(self):
        hospital1 = Hospital("123", "MyHospital")
        self.assertIsNotNone(hospital1)
        self.assertIsNotNone(hospital1.identifier)
        self.assertEqual(hospital1.identifier.value, TableNames.HOSPITAL.value + "/123")
        self.assertEqual(hospital1.identifier.use, IdUsages.ASSIGNED_BY_BETTER.value)

        hospital2 = Hospital(NONE_VALUE, "MyHospital")
        self.assertIsNotNone(hospital2)
        self.assertIsNotNone(hospital2.identifier)
        self.assertEqual(hospital2.identifier.value, TableNames.HOSPITAL.value + "/1")
        self.assertEqual(hospital2.identifier.use, IdUsages.ASSIGNED_BY_BETTER.value)

    def test_get_type(self):
        hospital1 = Hospital("123", "MyHospital")
        self.assertIsNotNone(hospital1)
        self.assertEqual(hospital1.get_type(), TableNames.HOSPITAL.value)

        hospital2 = Hospital(NONE_VALUE, "MyHospital")
        self.assertIsNotNone(hospital2)
        self.assertEqual(hospital2.get_type(), TableNames.HOSPITAL.value)

    def test_to_json(self):
        hospital1 = Hospital("123", "MyHospital")
        hospital1_json = hospital1.to_json()

        self.assertIsNotNone(hospital1_json)
        self.assertIn("identifier", hospital1_json)
        self.assertIn("value", hospital1_json["identifier"])
        self.assertEqual(hospital1_json["identifier"]["value"], TableNames.HOSPITAL.value + "/123")
        self.assertIn("use", hospital1_json["identifier"])
        self.assertEqual(hospital1_json["identifier"]["use"], IdUsages.ASSIGNED_BY_BETTER.value)
        self.assertIn("resourceType", hospital1_json)
        self.assertEqual(hospital1_json["resourceType"], TableNames.HOSPITAL.value)

    def test_from_json(self):
        hospital_patient = {
            "identifier": {
                "value": TableNames.HOSPITAL.value + "/123",
                "use": IdUsages.ASSIGNED_BY_BETTER.value
            },
            "resourceType": TableNames.HOSPITAL.value,
            "name": "MyHospital"
        }

        hospital = Hospital.from_json(hospital_patient)
        self.assertIsNotNone(hospital)
        self.assertIsNotNone(hospital.identifier)
        self.assertEqual(hospital.identifier.value, TableNames.HOSPITAL.value + "/123")
        self.assertEqual(hospital.identifier.use, IdUsages.ASSIGNED_BY_BETTER.value)
        self.assertEqual(hospital.get_type(), TableNames.HOSPITAL.value)


if __name__ == '__main__':
    unittest.main()
