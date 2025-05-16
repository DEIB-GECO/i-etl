from enums.EnumAsClass import EnumAsClass


class TheTestFiles(EnumAsClass):
    # for TEST files only, we set the absolute path with the Docker path
    # this is because tests are to be run in Docker only and only have to share the folder datasets/test

    # original files for the Extract step
    ORIG_METADATA_PATH = "orig-metadata.csv"  # this describes several hospitals, several datasets per hospital, etc
    ORIG_PHENOTYPIC_PATH = "orig-data-phen.csv"
    ORIG_CLINICAL_PATH = "orig-data-clin.csv"
    ORIG_DIAGNOSIS_PATH = "orig-data-diag.csv"
    ORIG_DYNAMIC_PATH = "orig-data-dyn.csv"
    ORIG_GENOMICS_PATH = "orig-data-gen.csv"
    # ORIG_EMPTY_PIDS_PATH = "orig-empty-pids.json"
    # ORIG_FILLED_PIDS_PATH = "orig-filled-pids.json"

    # files obtained after the Extract step
    # ued for the Transform step
    EXTR_METADATA_PHENOTYPIC_PATH = "extr-metadata-phen.csv"
    EXTR_METADATA_CLINICAL_PATH = "extr-metadata-clin.csv"
    EXTR_METADATA_DIAGNOSIS_PATH = "extr-metadata-diag.csv"
    EXTR_METADATA_DYNAMIC_PATH = "extr-metadata-dyn.csv"
    EXTR_METADATA_GENOMICS_PATH = "extr-metadata-gen.csv"
    EXTR_PHENOTYPIC_DATA_PATH = "extr-data-phen.csv"
    EXTR_CLINICAL_DATA_PATH = "extr-data-clin.csv"
    EXTR_DIAGNOSIS_DATA_PATH = "extr-data-diag.csv"
    EXTR_DYNAMIC_DATA_PATH = "extr-data-dyn.csv"
    EXTR_GENOMICS_DATA_PATH = "extr-data-gen.csv"
    EXTR_PHENOTYPIC_COL_CAT_PATH = "extr-data-phen-column-categorical.json"
    EXTR_CLINICAL_COL_CAT_PATH = "extr-data-clin-column-categorical.json"
    EXTR_PHENOTYPIC_UNITS_PATH = "extr-data-phen-column-to-unit.json"
    EXTR_CLINICAL_TYPE_PATH = "extr-data-clin-column-to-type.json"
    EXTR_PHENOTYPIC_TYPE_PATH = "extr-data-phen-column-to-type.json"
    EXTR_CLINICAL_UNITS_PATH = "extr-data-clin-column-to-unit.json"
    EXTR_PHENOTYPIC_DOMAIN_PATH = "extr-data-phen-column-to-domain.json"
    EXTR_CLINICAL_DOMAIN_PATH = "extr-data-clin-column-to-domain.json"

    PIDS_PATH = "anonymized_patient_ids.json"
    # EXTR_FILLED_PIDS_PATH = "extr-filled-pids.json"
