from enums.EnumAsClass import EnumAsClass
from utils.utils import normalize_column_name


class ColumnsToIgnore(EnumAsClass):
    ID = normalize_column_name("id")
    LINE = normalize_column_name("line")
    UNNAMED = normalize_column_name("unnamed")

