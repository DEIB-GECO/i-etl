# from src.database.Database import Database
# from src.etl.Extract import Extract
# from src.fhirDatatypes.CodeableConcept import CodeableConcept
# from src.profiles.Examination import Examination
# from src.profiles.Hospital import Hospital
# from src.profiles.Patient import Patient
# from src.utils.ExaminationCategory import ExaminationCategory
# from src.utils.Ontologies import Ontologies
# from src.utils.TableNames import TableNames
#
#
# class TestExtract:
#     pass
#     # def test_run(self):
#     #     self.fail()
#
#     # def test_load_existing_data_in_memory(self):
#     #     data_filepath = "data/test/manual.csv"
#     #     metadata_filepath = "data/test/metadata.csv"
#     #     database = Database("mongodb://localhost:27017/", "unit_tests")
#     #     patient1, patient2, patient3 = Patient("123"), Patient("456"), Patient("789")
#     #     hospital1, hospital2 = Hospital("1", "Hospital A"), Hospital("2", "Hospital B")
#     #     examination1 = Examination("1", CodeableConcept((Ontologies.LOINC.value["url"], "79731-6", "Left eye color vision")), "registered", ExaminationCategory.CATEGORY_CLINICAL.value)
#     #     examination2 = Examination("2", CodeableConcept((Ontologies.LOINC.value["url"], "79730-8", "Right eye color vision")), "registered", ExaminationCategory.CATEGORY_CLINICAL.value)
#     #     database.insert_many_tuples(table_name=TableNames.PATIENT.value, tuples=[patient1.to_json(), patient2.to_json(), patient3.to_json()])
#     #     database.insert_many_tuples(table_name=TableNames.HOSPITAL.value, tuples=[hospital1.to_json(), hospital2.to_json()])
#     #     database.insert_many_tuples(table_name=TableNames.EXAMINATION.value, tuples=[examination1.to_json(), examination2.to_json()])
#     #     extract = Extract(data_filepath, metadata_filepath, database, False)
#     #     extract.load_existing_data_in_memory()
#
#     # def test_load_metadata_file(self):
#     #     self.fail()
#     #
#     # def test_load_samples_file(self):
#     #     self.fail()
#     #
#     # def test_compute_mapped_values(self):
#     #     self.fail()
#     #
#     # def test_compute_mapped_types(self):
#     #     self.fail()
#     #
#     # def test_run_value_analysis(self):
#     #     self.fail()
#     #
#     # def test_run_variable_analysis(self):
#     #     self.fail()
