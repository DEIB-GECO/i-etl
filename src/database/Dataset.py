import uuid
from datetime import datetime

import jsonpickle

from constants.defaults import DATASET_GLOBAL_IDENTIFIER_PREFIX, NO_ID
from database.Counter import Counter
from database.Database import Database
from catalogue.DatasetProfile import DatasetProfile
from entities.Resource import Resource
from enums.TableNames import TableNames
from database.Operators import Operators
from utils.assertion_utils import is_not_nan
from utils.setup_logger import log


class Dataset(Resource):
    def __init__(self, database: Database, docker_path: str, version_notes: str, license: str, counter: Counter):
        super().__init__(id_value=NO_ID, entity_type=TableNames.DATASET, counter=counter)
        self.database = database
        self.docker_path = docker_path
        log.info(self.docker_path)
        results = self.database.find_operation(table_name=TableNames.DATASET, filter_dict={"docker_path": self.docker_path}, projection={})
        from_database = False
        for result in results:
            log.info(result)
            # there was a dataset
            self.global_identifier = result["global_identifier"]
            log.info(f"existing dataset identifier: {self.global_identifier}")
            self.version = str(int(result["version"]) + 1)  # increment the existing dataset version
            self.last_update = datetime.now().date()  # the release should be only computed the first time the dataset is inserted
            self.version_notes = result["version_notes"] if "version_notes" in result else None
            self.license = result["license"] if "license" in result else None
            from_database = True
        if not from_database:
            # no dataset was corresponding, initializing all fields
            self.global_identifier = Dataset.compute_global_identifier()
            log.info(f"new dataset identifier: {self.global_identifier}")
            self.version = "1"  # computed by incrementing the previous version (obtained from the db) - starts at 1
            self.release_date = datetime.now().date()
            self.last_update = datetime.now().date()
            self.version_notes = version_notes
            self.license = license
        log.info(self.global_identifier)
        self.profile = DatasetProfile(description="", theme="", filetype="", size=0, nb_tuples=0, completeness=0, uniqueness=0)

    @classmethod
    def compute_global_identifier(cls) -> str:
        return DATASET_GLOBAL_IDENTIFIER_PREFIX + str(uuid.uuid4())

    def __getstate__(self):
        # we need to check whether each field is a NaN value because we do not want to add fields for NaN values
        state = self.__dict__.copy()
        # trick: we need to work on the copy of the keys to not directly work on them
        # otherwise, Concurrent modification error
        for key in list(state.keys()):
            if state[key] is None or not is_not_nan(state[key]) or type(state[key]) is Database:
                del state[key]
            else:
                # we may also need to convert datetime within MongoDB-style dates
                if isinstance(state[key], datetime):
                    state[key] = Operators.from_datetime_to_isodate(state[key])
        return state

    def to_json(self):
        # encode creates a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
