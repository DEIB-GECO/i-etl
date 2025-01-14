from enums.EnumAsClass import EnumAsClass


class TimerKeys(EnumAsClass):
    TOTAL_TIME = "total_time"
    EXTRACT_TIME = "extract_time"
    TRANSFORM_TIME = "transform_time"
    LOAD_TIME = "load_time"
    STATISTICS_TIME = "statistics_time"
    REPORT_TIME = "report_time"
    OR_CREATION_TIME = "ontology_resource_creation_time"
    API_CALLS_TIME = "api_calls_time"
    DB_WRITE_TIME = "db_write_time"
    DB_BSON_SERIALIZATION_TIME = "bson_serialization_time"
