from enums.EnumAsClass import EnumAsClass


class QueryTypes(EnumAsClass):
    METADATA = "metadata"
    METADATA_NEW = "metadata_new"  # new access mode, get the whole metadata fields
    DATA = "data"
    DATA_NEW = "data_new"  # retrieves all fields matching ontology codes from a list; uses database field names
