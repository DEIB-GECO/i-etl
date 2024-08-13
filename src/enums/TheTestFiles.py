import os

from enums.EnumAsClass import EnumAsClass


class TheTestFiles(EnumAsClass):
    # original files for the Extract step
    ORIG_METADATA_PATH = os.path.join("datasets", "test", "orig-metadata.csv")  # this describes several hospitals, several datasets per hospitals, etc
    ORIG_LABORATORY_PATH = os.path.join("datasets", "test", "orig-data-lab.csv")
    ORIG_DISEASE_PATH = os.path.join("datasets", "test", "orig-data-dis.csv")
    ORIG_DYNAMIC_PATH = os.path.join("datasets", "test", "orig-data-dyn.csv")
    ORIG_GENOMICS_PATH = os.path.join("datasets", "test", "orig-data-gen.csv")
    ORIG_EMPTY_PIDS_PATH = os.path.join("datasets", "test", "orig-empty-pids.json")
    ORIG_FILLED_PIDS_PATH = os.path.join("datasets", "test", "orig-filled-pids.json")

    # files obtained after the Extract step
    # ued for the Transform step
    EXTR_METADATA_LABORATORY_PATH = os.path.join("datasets", "test", "extr-metadata-lab.csv")
    EXTR_METADATA_DISEASE_PATH = os.path.join("datasets", "test", "extr-metadata-dis.csv")
    EXTR_METADATA_DYNAMIC_PATH = os.path.join("datasets", "test", "extr-metadata-dyn.csv")
    EXTR_METADATA_GENOMICS_PATH = os.path.join("datasets", "test", "extr-metadata-gen.csv")
    EXTR_LABORATORY_DATA_PATH = os.path.join("datasets", "test", "extr-data-lab.csv")
    EXTR_DISEASE_DATA_PATH = os.path.join("datasets", "test", "extr-data-dis.csv")
    EXTR_DYNAMIC_DATA_PATH = os.path.join("datasets", "test", "extr-data-dyn.csv")
    EXTR_GENOMICS_DATA_PATH = os.path.join("datasets", "test", "extr-data-gen.csv")
    EXTR_LABORATORY_CATEGORICAL_PATH = os.path.join("datasets", "test", "extr-data-lab-categorical.json")
    EXTR_LABORATORY_COL_CAT_PATH = os.path.join("datasets", "test", "extr-data-lab-column-categorical.json")
    EXTR_GENOMICS_CATEGORICAL_PATH = os.path.join("datasets", "test", "extr-data-gen-categorical.json")
    EXTR_LABORATORY_DIMENSIONS_PATH = os.path.join("datasets", "test", "extr-data-lab-column-to-dimension.json")
    EXTR_EMPTY_PIDS_PATH = os.path.join("datasets", "test", "extr-empty-pids.json")
    EXTR_FILLED_PIDS_PATH = os.path.join("datasets", "test", "extr-filled-pids.json")
