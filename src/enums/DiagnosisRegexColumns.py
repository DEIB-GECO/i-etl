from enums.EnumAsClass import EnumAsClass
from utils.utils import normalize_column_name


class DiagnosisRegexColumns(EnumAsClass):
    DIAGNOSIS_NAME = normalize_column_name(column_name="Diagnosis name")
    ACRONYM = normalize_column_name(column_name="Acronym")
    OMIM_CODE = normalize_column_name(column_name="OMIM ID")
    ORPHANET_CODE = normalize_column_name(column_name="OrphaNet")
    REGEX_PATTERN = normalize_column_name(column_name="regex pattern")


