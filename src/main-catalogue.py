import os

from dotenv import load_dotenv

from catalogue.CatalogueUpdate import CatalogueUpdate
from database.Database import Database
from database.Execution import Execution


if __name__ == '__main__':
    # A. load the env. variables defined in .env.
    # note: specifying the .env in the compose.yml only gives access to those env. var. to Docker (not to Python)
    load_dotenv(os.environ["MY_ENV_FILE"])

    execution = Execution()
    execution.internals_set_up()
    execution.file_set_up(setup_files=False)
    database = Database(execution=execution)
    catalogue_update = CatalogueUpdate(database=database)
    catalogue_update.run()
