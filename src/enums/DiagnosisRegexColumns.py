from enums.EnumAsClass import EnumAsClass
from enums.MetadataColumns import MetadataColumns


class DiagnosisRegexColumns(EnumAsClass):
    DIAGNOSIS_NAME = MetadataColumns.normalize_name(column_name="Diagnosis name")
    ACRONYM = MetadataColumns.normalize_name(column_name="Acronym")
    OMIM_CODE = MetadataColumns.normalize_name(column_name="OMIM ID")
    ORPHANET_CODE = MetadataColumns.normalize_name(column_name="OrphaNet")
    REGEX_PATTERN = MetadataColumns.normalize_name(column_name="regex pattern")


