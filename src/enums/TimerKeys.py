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
    LOAD_JSON_IN_MONGO = "load_json_in_mongo"
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
    PROCESS_RECORD_BATCH = "process_record_batch"
    WRITE_RECORDS_IN_DB = "write_records_in_db"

    # Fairification
    VALUE_FAIRIFICATION = "value_fairification"
    GET_COLUMN_INFO = "get_column_info"
    GET_ETL_TYPE = "get_etl_type"
    GET_ETL_UNIT = "get_etl_unit"
    FAIRIFY_STR = "nb_fairify_str"
    FAIRIFY_CATEGORY = "fairify_category"
    FAIRIFY_API = "fairify_api"
    FAIRIFY_DATES = "fairify_dates"
    FAIRIFY_BOOLEAN = "fairify_boolean"
    FAIRIFY_NUM = "fairify_numeric"

    # Counters
    NB_FAIRIFY_STR = "nb_fairify_str"
    NB_FAIRIFY_CATEGORY = "nb_fairify_category"
    NB_FAIRIFY_API = "nb_fairify_api"
    NB_FAIRIFY_DATES = "nb_fairify_dates"
    NB_FAIRIFY_BOOLEAN = "nb_fairify_boolean"
    NB_FAIRIFY_NUM = "nb_fairify_numeric"

    # Writing record resources
    GET_RESOURCE_JSON_FILE = "get_resource_json_file"
    JSONIFY_RECORDS = "jsonify_records"
    USJON_DUMP = "ujson_dump"

