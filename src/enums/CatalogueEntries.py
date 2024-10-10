from enums.EnumAsClass import EnumAsClass


class CatalogueEntries(EnumAsClass):
    FEATURE_ID = "id"
    FEATURE_NAME = "name"
    FEATURE_CODES = "codes"
    FEATURE_TYPE = "type"
    FEATURE_AGG_TYPE = "agg_type"
    FEATURE_ENTITY = "entity"

    STATS_REC_COUNT = "record_count"
    STATS_PERC_EMPTY_CELLS = "perc_empty_cells"
    STATS_PERC_MATCHING_TYPES = "perc_data_matching_types"
    UNCATEGORIZED_CATEGORIES = "unrecognized_categories"

    AGG_VALUES = "values"
    AGG_COUNTS = "counts"
    AGG_MIN = "min"
    AGG_MAX = "max"
    AGG_AVG = "avg"
    AGG_MEAN = "mean"
