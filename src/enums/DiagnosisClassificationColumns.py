from enums.EnumAsClass import EnumAsClass
from utils.utils import normalize_column_name


class DiagnosisClassificationColumns(EnumAsClass):
    DIAGNOSIS_FREE_TEXT = normalize_column_name(column_name="Diagnosis")
    DIAGNOSIS_CLASSIFICATION = normalize_column_name(column_name="Classification")
    DIAGNOSIS_NAME = normalize_column_name(column_name="Diagnosis name")
