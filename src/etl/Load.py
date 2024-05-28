from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Transform import Transform
from src.utils.TableNames import TableNames


class Load():
    def __init__(self, extract: Extract, transform: Transform, database: Database):
        self.extract = extract
        self.transform = transform
        self.database = database

    def run(self):
        patients_json_list = [patient.to_json() for patient in self.transform.patients]
        self.database.insert_many_tuples(table_name=TableNames.PATIENT.value, tuples=patients_json_list)
        examinations_json_list = [examination.to_json() for examination in self.transform.examinations]
        self.database.insert_many_tuples(table_name=TableNames.EXAMINATION.value, tuples=examinations_json_list)
        examination_records_json_list = [examination_record.to_json() for examination_record in self.transform.examination_records]
        self.database.insert_many_tuples(table_name=TableNames.EXAMINATION_RECORD.value, tuples=examination_records_json_list)

    def __create_db_indexes(self):
        self.database.create_unique_index(table_name=TableNames.PATIENT.value, columns={"metadata.csv_filepath": 1, "metadata.csv_line": 1})
        self.database.create_unique_index(table_name=TableNames.HOSPITAL.value, columns={"id": 1, "name": 1})
        self.database.create_unique_index(table_name=TableNames.EXAMINATION.value, columns={"code.codings.system": 1, "code.codings.code": 1})
        self.database.create_unique_index(table_name=TableNames.EXAMINATION_RECORD.value, columns={"instantiate.reference": 1, "subject.reference": 1, "recorded_by.reference": 1, "value": 1, "issued": 1})
