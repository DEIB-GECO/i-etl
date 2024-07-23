from utils.utils import normalize_column_name


class ColumnsToIgnore:
    ID = normalize_column_name("id")
    LINE = normalize_column_name("line")
    UNNAMED = normalize_column_name("unnamed")

    @classmethod
    def values(cls):
        xs = []
        for name, value in vars(cls).items():
            if not (name.startswith('__') or isinstance(value, classmethod)):
                xs.append(value)
        return xs
