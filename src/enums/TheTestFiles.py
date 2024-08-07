import os

from enums.EnumAsClass import EnumAsClass


class TheTestFiles(EnumAsClass):
    # original files for the Extract step
    TEST_ORIG_METADATA_PATH = os.path.join("datasets", "test", "test-orig-metadata.csv")  # this describes several hospitals, several datasets per hospitals, etc
    TEST_ORIG_LABORATORY_PATH = os.path.join("datasets", "test", "test-orig-laboratory-data.csv")
    TEST_ORIG_DISEASE_PATH = os.path.join("datasets", "test", "test-orig-disease-data.csv")
    TEST_ORIG_DYNAMIC_PATH = os.path.join("datasets", "test", "test-orig-dynamic-data.csv")
    TEST_ORIG_GENOMICS_PATH = os.path.join("datasets", "test", "test-orig-genomics-data.csv")

    # files obtained after the Extract step
    # ued for the Transform step
    TEST_EXTR_METADATA_LABORATORY_PATH = os.path.join("datasets", "test", "test-extr-metadata-laboratory.csv")
    TEST_EXTR_METADATA_DISEASE_PATH = os.path.join("datasets", "test", "test-extr-metadata-disease.csv")
    TEST_EXTR_METADATA_DYNAMIC_PATH = os.path.join("datasets", "test", "test-extr-metadata-dynamic.csv")
    TEST_EXTR_METADATA_GENOMICS_PATH = os.path.join("datasets", "test", "test-extr-metadata-genomics.csv")
    TEST_EXTR_LABORATORY_DATA_PATH = os.path.join("datasets", "test", "test-extr-laboratory-data.csv")
    TEST_EXTR_DISEASE_DATA_PATH = os.path.join("datasets", "test", "test-extr-disease-data.csv")
    TEST_EXTR_DYNAMIC_DATA_PATH = os.path.join("datasets", "test", "test-extr-dynamic-data.csv")
    TEST_EXTR_GENOMICS_DATA_PATH = os.path.join("datasets", "test", "test-extr-genomics-data.csv")
    TEST_EXTR_LABORATORY_CATEGORICAL_PATH = os.path.join("datasets", "test", "test-extr-categorical-laboratory-data.json")
    TEST_EXTR_GENOMICS_CATEGORICAL_PATH = os.path.join("datasets", "test", "test-extr-categorical-genomics-data.json")
    TEST_EXTR_LABORATORY_DIMENSIONS_PATH = os.path.join("datasets", "test", "test-extr-column-to-dimension-laboratory-data.json")
