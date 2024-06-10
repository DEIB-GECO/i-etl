import unittest

from src.profiles.Hospital import Hospital
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE


class TestHospital(unittest.TestCase):
    def test_constructor(self):
        hospital1 = Hospital("123", "MyHospital")
        self.assertIsNotNone(hospital1)
        self.assertIsNotNone(hospital1.identifier)
        self.assertEqual(hospital1.identifier, TableNames.HOSPITAL.value + "/123")

        hospital2 = Hospital(NONE_VALUE, "MyHospital")
        self.assertIsNotNone(hospital2)
        self.assertIsNotNone(hospital2.identifier)
        self.assertEqual(hospital2.identifier, TableNames.HOSPITAL.value + "/1")

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
        self.assertEqual(hospital1_json["identifier"], TableNames.HOSPITAL.value + "/123")
        self.assertIn("resourceType", hospital1_json)
        self.assertEqual(hospital1_json["resourceType"], TableNames.HOSPITAL.value)


if __name__ == '__main__':
    unittest.main()
