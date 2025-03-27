import dataclasses
import os.path
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

    # keys to be used when writing JSON or queries
    # those names have to exactly match the variables names declared in entity classes
    GID_ = "global_identifier"

    # keys to be used when using an entity attribute as a query variable
    GID__ = f"${GID_}"

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
                self.last_update = datetime.now()  # the release should be only computed the first time the dataset is inserted
                self.version_notes = result["version_notes"] if "version_notes" in result else None
                self.license = result["license"] if "license" in result else None
                from_database = True
        if not from_database:
            # no dataset was corresponding, initializing all fields
            self.global_identifier = Dataset.compute_global_identifier()
            log.info(f"new dataset identifier: {self.global_identifier}")
            self.version = "1"  # computed by incrementing the previous version (obtained from the db) - starts at 1
            self.release_date = datetime.now()
            self.last_update = datetime.now()
        log.info(self.global_identifier)
        # we get the size in Mb and round it to 6 digits
        size = round(os.path.getsize(self.docker_path)/(1000*1000), 6)
        self.profile = DatasetProfile(description="", theme="",
                                      filetype=self.get_file_type(self.docker_path), size=size, nb_tuples=0,
                                      completeness=0, uniqueness=0)

    @classmethod
    def get_file_type(cls, docker_path) -> str:
        _, file_extension = os.path.splitext(docker_path)
        return file_extension[1:]  # remove the comma kept at the beginning of the extension

    @classmethod
    def compute_global_identifier(cls) -> str:
        return DATASET_GLOBAL_IDENTIFIER_PREFIX + str(uuid.uuid4())
