from enums.EnumAsClass import EnumAsClass


class TheTestFiles(EnumAsClass):
    # for TEST files only, we set the absolute path with the Docker path
    # this is because tests are to be run in Docker only and only have to share the folder datasets/test

    # original files for the Extract step
    ORIG_METADATA_PATH = "orig-metadata.csv"  # this describes several hospitals, several datasets per hospitals, etc
    ORIG_LABORATORY_PATH = "orig-data-lab.csv"
    ORIG_DISEASE_PATH = "orig-data-dis.csv"
    ORIG_DYNAMIC_PATH = "orig-data-dyn.csv"
    ORIG_GENOMICS_PATH = "orig-data-gen.csv"
    ORIG_EMPTY_PIDS_PATH = "orig-empty-pids.json"
    ORIG_FILLED_PIDS_PATH = "orig-filled-pids.json"
    ORIG_DIAGNOSIS_TO_CC_PATH = "orig-diagnosis-regexes.xlsx"

    # files obtained after the Extract step
    # ued for the Transform step
    EXTR_METADATA_LABORATORY_PATH = "extr-metadata-lab.csv"
    EXTR_METADATA_DISEASE_PATH = "extr-metadata-dis.csv"
    EXTR_METADATA_DYNAMIC_PATH = "extr-metadata-dyn.csv"
    EXTR_METADATA_GENOMICS_PATH = "extr-metadata-gen.csv"
    EXTR_LABORATORY_DATA_PATH = "extr-data-lab.csv"
    EXTR_DISEASE_DATA_PATH = "extr-data-dis.csv"
    EXTR_DYNAMIC_DATA_PATH = "extr-data-dyn.csv"
    EXTR_GENOMICS_DATA_PATH = "extr-data-gen.csv"
    EXTR_LABORATORY_CATEGORICAL_PATH = "extr-data-lab-categorical.json"
    EXTR_LABORATORY_COL_CAT_PATH = "extr-data-lab-column-categorical.json"
    EXTR_GENOMICS_CATEGORICAL_PATH = "extr-data-gen-categorical.json"
    EXTR_LABORATORY_DIMENSIONS_PATH = "extr-data-lab-column-to-dimension.json"
    EXTR_JSON_DIAGNOSIS_TO_CC_PATH = "extr-data-diag-to-cc.json"
    EXTR_EMPTY_PIDS_PATH = "extr-empty-pids.json"
    EXTR_FILLED_PIDS_PATH = "extr-filled-pids.json"
