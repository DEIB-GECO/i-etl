from enums.EnumAsClass import EnumAsClass


class TheTestFiles(EnumAsClass):
    # for TEST files only, we set the absolute path with the Docker path
    # this is because tests are to be run in Docker only and only have to share the folder datasets/test

    # original files for the Extract step
    ORIG_METADATA_PATH = "orig-metadata.csv"  # this describes several hospitals, several datasets per hospital, etc
    ORIG_PHENOTYPIC_PATH = "orig-data-phen.csv"
    ORIG_SAMPLE_PATH = "orig-data-sam.csv"
    ORIG_DISEASE_PATH = "orig-data-dis.csv"
    ORIG_DYNAMIC_PATH = "orig-data-dyn.csv"
    ORIG_GENOMICS_PATH = "orig-data-gen.csv"
    ORIG_EMPTY_PIDS_PATH = "orig-empty-pids.json"
    ORIG_FILLED_PIDS_PATH = "orig-filled-pids.json"
    ORIG_DIAGNOSIS_TO_CC_PATH = "orig-diagnosis-regexes.xlsx"

    # files obtained after the Extract step
    # ued for the Transform step
    EXTR_METADATA_PHENOTYPIC_PATH = "extr-metadata-phen.csv"
    EXTR_METADATA_SAMPLE_PATH = "extr-metadata-sam.csv"
    EXTR_METADATA_DISEASE_PATH = "extr-metadata-dis.csv"
    EXTR_METADATA_DYNAMIC_PATH = "extr-metadata-dyn.csv"
    EXTR_METADATA_GENOMICS_PATH = "extr-metadata-gen.csv"
    EXTR_PHENOTYPIC_DATA_PATH = "extr-data-phen.csv"
    EXTR_SAMPLE_DATA_PATH = "extr-data-sam.csv"
    EXTR_DISEASE_DATA_PATH = "extr-data-dis.csv"
    EXTR_DYNAMIC_DATA_PATH = "extr-data-dyn.csv"
    EXTR_GENOMICS_DATA_PATH = "extr-data-gen.csv"
    EXTR_PHENOTYPIC_CATEGORICAL_PATH = "extr-data-phen-categorical.json"
    EXTR_SAMPLE_CATEGORICAL_PATH = "extr-data-sam-categorical.json"
    EXTR_PHENOTYPIC_COL_CAT_PATH = "extr-data-phen-column-categorical.json"
    EXTR_SAMPLE_COL_CAT_PATH = "extr-data-sam-column-categorical.json"
    EXTR_GENOMICS_CATEGORICAL_PATH = "extr-data-gen-categorical.json"
    EXTR_PHENOTYPIC_DIMENSIONS_PATH = "extr-data-phen-column-to-dimension.json"
    EXTR_SAMPLE_DIMENSIONS_PATH = "extr-data-sam-column-to-dimension.json"
    EXTR_JSON_DIAGNOSIS_TO_OT_PATH = "extr-data-diag-to-or.json"
    EXTR_EMPTY_PIDS_PATH = "extr-empty-pids.json"
    EXTR_FILLED_PIDS_PATH = "extr-filled-pids.json"
