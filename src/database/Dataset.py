import dataclasses
import uuid
from datetime import datetime

from catalogue.DatasetProfile import DatasetProfile
from constants.defaults import DATASET_GLOBAL_IDENTIFIER_PREFIX
from database.Database import Database
from entities.Resource import Resource
from enums.TableNames import TableNames
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class Dataset(Resource):
    database: Database = dataclasses.field(repr=False)
    docker_path: str
    global_identifier: str = dataclasses.field(init=False)
    version: str = dataclasses.field(init=False)
    release_date: datetime.date = dataclasses.field(init=False)
    last_update: datetime.date = dataclasses.field(init=False)
    version_notes: str
    license: str
    profile: DatasetProfile = dataclasses.field(init=False)

    def __post_init__(self):
        super().__post_init__()
        log.info(self.docker_path)
        from_database = False
        if self.database is not None:
            results = self.database.find_operation(table_name=TableNames.DATASET, filter_dict={"docker_path": self.docker_path}, projection={})
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
        log.info(self.global_identifier)
        self.profile = DatasetProfile(description="", theme="", filetype="", size=0, nb_tuples=0, completeness=0, uniqueness=0)

    @classmethod
    def compute_global_identifier(cls) -> str:
        return DATASET_GLOBAL_IDENTIFIER_PREFIX + str(uuid.uuid4())
