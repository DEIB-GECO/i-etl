from catalogue.CatalogueUpdate import CatalogueUpdate
from database.Database import Database
from database.Execution import Execution


if __name__ == '__main__':
    execution = Execution()
    execution.set_up(setup_data_files=False)
    database = Database(execution=execution)
    catalogue_update = CatalogueUpdate(db=database)
    catalogue_update.compute_data_for_catalogue()
