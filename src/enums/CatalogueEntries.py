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
    FEATURE_ONTOLOGY_CODE = "ontology"
    # FEATURE_ONTOLOGY_LABEL = "ontology_label"
    FEATURE_DATA_TYPE = "data_type"
    FEATURE_VISIBILITY = "visibility"
    FEATURE_ENTITY_TYPE = "entity_type"
    FEATURE_AGG_TYPE = "agg_type"

    # Domain entity
    DOMAIN_MIN = "min"
    DOMAIN_MAX = "max"
    DOMAIN_CAT_ACCEPTED = "accepted_values"
