from enums.EnumAsClass import EnumAsClass


class CatalogueProfileEntries(EnumAsClass):

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
