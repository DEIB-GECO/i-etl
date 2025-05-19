from enums.EnumAsClass import EnumAsClass


class ParameterKeys(EnumAsClass):
    # this corresponds to (almost) all the keys defined in the .env
    HOSPITAL_NAME = "HOSPITAL_NAME"
    DB_NAME = "DB_NAME"
    DB_DROP = "DB_DROP"
    USE_LOCALE = "USE_LOCALE"
    COLUMNS_TO_REMOVE_KEY = "COLUMNS_TO_REMOVE"
    METADATA_PATH = "METADATA"
    DATA_FILES = "DATA_FILES"
    ANONYMIZED_PATIENT_IDS = "ANONYMIZED_PIDS"
    RECORD_CARRIER_PATIENT = "RECORD_CARRIER_PATIENTS"
    PATIENT_ID_COLUMN = "PATIENT_ID"
    SAMPLE_ID_COLUMN = "SAMPLE_ID"
