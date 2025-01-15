from enums.EnumAsClass import EnumAsClass


class TimerKeys(EnumAsClass):
    TOTAL_TIME = "total_time"
    EXTRACT_TIME = "extract_time"
    TRANSFORM_TIME = "transform_time"
    LOAD_TIME = "load_time"
    STATISTICS_TIME = "statistics_time"
    REPORT_TIME = "report_time"
    INSERT_DATASETS = "insert_datasets"
    PROFILE_COMPUTATION = "profile_computation"
    READ_TABULAR = "read_tabular"
    NORMALIZATION = "normalization"
    COMPUTE_MAPPINGS = "compute_mappings"
    OR_CREATION_TIME = "ontology_resource_creation_time"
    API_CALLS_TIME = "api_calls_time"
    DB_WRITE_TIME = "db_write_time"
    DB_BSON_SERIALIZATION_TIME = "bson_serialization_time"
    UPSERT_TUPLES = "upsert_tuples"

    # Transform
    CREATE_PATIENTS = "create_patients"
    WRITE_PATIENTS_IN_DB = "write_patients_in_db"
    CREATE_FEATURES = "create_features"
    CREATE_FEATURE_INSTANCES = "create_feature_instances"
    PROCESS_FEATURE_BATCH = "process_feature_batch"
    WRITE_FEATURES_IN_DB = "write_features_in_db"
    CREATE_RECORDS = "create_records"
    CREATE_RECORD_INSTANCES = "create_record_instances"
    VALUE_FAIRIFICATION = "value_fairification"
    PROCESS_RECORD_BATCH = "process_record_batch"
    WRITE_RECORDS_IN_DB = "write_records_in_db"
