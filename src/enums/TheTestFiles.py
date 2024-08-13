import os

from enums.EnumAsClass import EnumAsClass


class TheTestFiles(EnumAsClass):
    # original files for the Extract step
    TEST_ORIG_METADATA_PATH = os.path.join("datasets", "test", "orig-metadata.csv")  # this describes several hospitals, several datasets per hospitals, etc
    TEST_ORIG_LABORATORY_PATH = os.path.join("datasets", "test", "orig-data-lab.csv")
    TEST_ORIG_DISEASE_PATH = os.path.join("datasets", "test", "orig-data-dis.csv")
    TEST_ORIG_DYNAMIC_PATH = os.path.join("datasets", "test", "orig-data-dyn.csv")
    TEST_ORIG_GENOMICS_PATH = os.path.join("datasets", "test", "orig-data-gen.csv")

    # files obtained after the Extract step
    # ued for the Transform step
    TEST_EXTR_METADATA_LABORATORY_PATH = os.path.join("datasets", "test", "extr-metadata-lab.csv")
    TEST_EXTR_METADATA_DISEASE_PATH = os.path.join("datasets", "test", "extr-metadata-dis.csv")
    TEST_EXTR_METADATA_DYNAMIC_PATH = os.path.join("datasets", "test", "extr-metadata-dyn.csv")
    TEST_EXTR_METADATA_GENOMICS_PATH = os.path.join("datasets", "test", "extr-metadata-gen.csv")
    TEST_EXTR_LABORATORY_DATA_PATH = os.path.join("datasets", "test", "extr-data-lab.csv")
    TEST_EXTR_DISEASE_DATA_PATH = os.path.join("datasets", "test", "extr-data-dis.csv")
    TEST_EXTR_DYNAMIC_DATA_PATH = os.path.join("datasets", "test", "extr-data-dyn.csv")
    TEST_EXTR_GENOMICS_DATA_PATH = os.path.join("datasets", "test", "extr-data-gen.csv")
    TEST_EXTR_LABORATORY_CATEGORICAL_PATH = os.path.join("datasets", "test", "extr-data-lab-categorical.json")
    TEST_EXTR_LABORATORY_COL_CAT_PATH = os.path.join("datasets", "test", "extr-data-lab-column-categorical.json")
    TEST_EXTR_GENOMICS_CATEGORICAL_PATH = os.path.join("datasets", "test", "extr-data-gen-categorical.json")
    TEST_EXTR_LABORATORY_DIMENSIONS_PATH = os.path.join("datasets", "test", "extr-data-lab-column-to-dimension.json")
