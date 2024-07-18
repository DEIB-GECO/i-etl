import os
from enum import Enum


class TheTestFiles(Enum):
    # original files for the Extract step
    TEST_ORIG_METADATA_PATH = os.path.join("datasets", "test", "test-orig-metadata.csv")  # this describes several hospitals, several datasets per hospitals, etc
    TEST_ORIG_CLINICAL_PATH = os.path.join("datasets", "test", "test-orig-clinical-data.csv")
    TEST_ORIG_DISEASE_PATH = os.path.join("datasets", "test", "test-orig-disease-data.csv")
    TEST_ORIG_DYNAMIC_PATH = os.path.join("datasets", "test", "test-orig-dynamic-data.csv")
    TEST_ORIG_GENOMICS_PATH = os.path.join("datasets", "test", "test-orig-genomics-data.csv")

    # files obtained after the Extract step
    # ued for the Transform step
    TEST_EXTR_METADATA_CLINICAL_PATH = os.path.join("datasets", "test", "test-extr-metadata-clinical.csv")
    TEST_EXTR_METADATA_DISEASE_PATH = os.path.join("datasets", "test", "test-extr-metadata-disease.csv")
    TEST_EXTR_METADATA_DYNAMIC_PATH = os.path.join("datasets", "test", "test-extr-metadata-dynamic.csv")
    TEST_EXTR_METADATA_GENOMICS_PATH = os.path.join("datasets", "test", "test-extr-metadata-genomics.csv")
    TEST_EXTR_CLINICAL_DATA_PATH = os.path.join("datasets", "test", "test-extr-clinical-data.csv")
    TEST_EXTR_DISEASE_DATA_PATH = os.path.join("datasets", "test", "test-extr-disease-data.csv")
    TEST_EXTR_DYNAMIC_DATA_PATH = os.path.join("datasets", "test", "test-extr-dynamic-data.csv")
    TEST_EXTR_GENOMICS_DATA_PATH = os.path.join("datasets", "test", "test-extr-genomics-data.csv")
    TEST_EXTR_CLINICAL_MAPPED_PATH = os.path.join("datasets", "test", "test-extr-mapped-values-clinical-data.json")
    TEST_EXTR_GENOMICS_MAPPED_PATH = os.path.join("datasets", "test", "test-extr-mapped-values-genomics-data.json")

    # files obtained after the Transform step
    # used for the Load step
