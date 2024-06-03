import json
import os
import re

from src.database.Database import Database
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log


class Load:
    def __init__(self, database: Database):
        self.database = database

    def run(self):
        # Insert resources that have not been inserted yet, i.e.,
        # anything else than Hospital, Examination and Disease instances
        log.debug("in the Load class")
        self.load_json_in_table(table_name=TableNames.PATIENT.value, unique_variables=["identifier"])

        self.load_json_in_table(table_name=TableNames.EXAMINATION_RECORD.value, unique_variables=["recordedBy", "subject", "basedOn", "instantiate"])

        self.load_json_in_table(table_name=TableNames.SAMPLE.value, unique_variables=["identifier"])

    def load_json_in_table(self, table_name: str, unique_variables):
        for filename in os.listdir(os.path.join("working-dir", "files")):
            if re.search(table_name+"[0-9]+", filename) is not None:
                # implementation note: we cannot simply use filename.startswith(table_name)
                # because both Examination and ExaminationRecord start with Examination
                # the solution is to use a regex
                with open(os.path.join("working-dir", "files", filename), "r") as json_datafile:
                    tuples = json.load(json_datafile)
                    log.debug("Table %s, file %s, loading %s tuples", table_name, filename, len(tuples))
                    self.database.upsert_batch_of_tuples(table_name=table_name,
                                                         unique_variables=unique_variables,
                                                         tuples=tuples)

    def retrieve_identifiers(self, table_name: str, projection: str):
        return self.database.retrieve_identifiers(table_name=table_name, projection=projection)

    def create_db_indexes(self):
        self.database.create_unique_index(table_name=TableNames.PATIENT.value, columns={"metadata.csv_filepath": 1, "metadata.csv_line": 1})
        self.database.create_unique_index(table_name=TableNames.HOSPITAL.value, columns={"id": 1, "name": 1})
        self.database.create_unique_index(table_name=TableNames.EXAMINATION.value, columns={"code.codings.system": 1, "code.codings.code": 1})
        self.database.create_unique_index(table_name=TableNames.EXAMINATION_RECORD.value, columns={"instantiate.reference": 1, "subject.reference": 1, "recorded_by.reference": 1, "value": 1, "issued": 1})
