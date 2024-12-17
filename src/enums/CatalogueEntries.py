from enums.EnumAsClass import EnumAsClass


class CatalogueEntries(EnumAsClass):
    # Hospital entity
    HOSPITAL_ID = "hospital_id"  # TODO
    HOSPITAL_STATION_NAME = "station_name"
    HOSPITAL_RESPONSIBLE_STATION = "responsible_for_station"
    HOSPITAL_CONTACT_POINTS = "contact_points"
    HOSPITAL_STATION_CREATOR = "station_creator"
    HOSPITAL_DESCRIPTION = "description"
    HOSPITAL_LOCATION = "location"
    HOSPITAL_CERTIFICATIONS = "certifications"
    HOSPITAL_STATION_CREATION_DATE = "station_creation_date"

    # Dataset entity
    DATASET_ID = "dataset_id"
    DATASET_VERSION = "version"
    DATASET_RELEASE_DATE = "release_date"
    DATASET_LAST_UPDATE_DATE = "last_update_date"
    DATASET_VERSION_NOTES = "version_notes"
    DATASET_LICENSE = "license"

    # Dataset profile entity
    DS_PROFILE_ID = "profile_id"
    DS_PROFILE_DESCRIPTION = "description"
    DS_PROFILE_THEME = "theme"
    DS_PROFILE_FILE_TYPE = "file_type"
    DS_PROFILE_SIZE = "size_in_mb"
    DS_PROFILE_NB_TUPLES = "nb_tuples"
    DS_PROFILE_TUPLE_COMPLETENESS = "tuple_completeness"
    DS_PROFILE_TUPLE_UNIQUENESS = "tuple_uniqueness"

    # Feature entity
    FEATURE_ID = "feature_id"
    FEATURE_NAME = "name"
    FEATURE_DESCRIPTION = "description"
    FEATURE_ONTOLOGY_CODE = "ontology_code"
    FEATURE_ONTOLOGY_LABEL = "ontology_label"
    FEATURE_DATA_TYPE = "data_type"
    FEATURE_VISIBILITY = "visibility"
    FEATURE_ENTITY_TYPE = "entity_type"
    FEATURE_AGG_TYPE = "agg_type"

    # Domain entity
    DOMAIN_NUM_MIN = "min_value"
    DOMAIN_NUM_MAX = "max_value"
    DOMAIN_CAT_ACCEPTED = "accepted_values"
    DOMAIN_DATE_MIN = "min_date"
    DOMAIN_DATE_MAX = "max_date"

    # Profile entity
    F_PROFILE_ID = "profile_id"
    F_PROFILE_ENTROPY = "entropy"
    F_PROFILE_DENSITY = "density"
    F_PROFILE_MAP_VALUE_COUNTS = "values_and_counts"
    F_PROFILE_MISSING_PERC = "missing_percentage"
    F_PROFILE_DT_VALIDITY = "data_type_validity"
    F_PROFILE_UNIQUENESS = "uniqueness"
    F_PROFILE_ACCURACY_SCORE = "accuracy_score"

    # Numeric Profile entity
    F_NUM_PROFILE_MIN = "min"
    F_NUM_PROFILE_MAX = "max"
    F_NUM_PROFILE_MEAN = "mean"
    F_NUM_PROFILE_MEDIAN = "median"
    F_NUM_PROFILE_STD_DEV = "standard_deviation"
    F_NUM_PROFILE_SKEWNESS = "skewness"
    F_NUM_PROFILE_KURTOSIS = "kurtosis"
    F_NUM_PROFILE_MED_ABS_DEV = "median_absolute_deviation"
    F_NUM_PROFILE_INTER_QU_RANGE = "inter_quartile_range"
    F_NUM_PROFILE_CORRELATION = "correlation_matrix"

    # Date Profile entity
    F_DATE_PROFILE_MIN = "min"
    F_DATE_PROFILE_MAX = "max"
    F_DATE_PROFILE_MEDIAN = "median"
    F_DATE_PROFILE_INTER_QU_RANGE = "inter_quartile_range"

    # Categorical Profile entity
    F_CAT_PROFILE_IMBALANCE = "imbalance"
    F_CAT_PROFILE_CONSTANCY = "constancy"
    F_CAT_PROFILE_MODE = "mode"

    # STATS_REC_COUNT = "record_count"
    # STATS_PERC_EMPTY_CELLS = "perc_empty_cells"
    # STATS_PERC_MATCHING_TYPES = "perc_data_matching_types"
    # UNCATEGORIZED_CATEGORIES = "unrecognized_categories"
    # AGG_VALUES = "values"
    # AGG_COUNTS = "counts"
    # AGG_MIN = "min"
    # AGG_MAX = "max"
    # AGG_AVG = "avg"
    # AGG_MEAN = "mean"
