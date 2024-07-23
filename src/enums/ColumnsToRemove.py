from enum import Enum

from utils.utils import normalize_column_name


class ColumnsToRemove(Enum):
    ID = normalize_column_name("id")
    LINE = normalize_column_name("line")
    UNNAMED = normalize_column_name("unnamed")

    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))
