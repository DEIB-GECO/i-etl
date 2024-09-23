import locale
from datetime import datetime

from dateutil.parser import parse

from enums.MetadataColumns import MetadataColumns


def cast_str_to_int(str_value: str):
    try:
        return int(str_value)
    except:
        if str_value.endswith(".0"):
            # this is a float value, that we can safely convert to int
            # this may happen when:
            # (a) Pandas converted into cell to float (because of NaN probably)
            # (b) or the data is really a float, that we can cast further
            return int(str_value.replace(".0", ""))
        else:
            # this was not an int value
            return None


def cast_str_to_float(str_value: str):
    try:
        return locale.atof(str_value)
    except:
        return None  # this was not a float value


def cast_str_to_boolean(str_value: str):
    # try to convert as boolean
    normalized_value = MetadataColumns.normalize_value(column_value=str_value)
    if normalized_value == "true" or normalized_value == "1" or normalized_value == "1.0" or normalized_value == "yes":
        return True
    elif normalized_value == "false" or normalized_value == "0" or normalized_value == "0.0" or normalized_value == "no":
        return False
    else:
        return None  # this was not a boolean value


def cast_str_to_datetime(str_value: str) -> datetime:
    try:
        datetime_value = parse(str_value)
        # %Y-%m-%d %H:%M:%S is the format used by default by parse (the output is always of this form)
        return datetime_value
    except:  # this may raise ValueError if this is not a date, or OverflowError in case of weird int (BUZZI data)
        # this was not a datetime value, and we signal it with None
        return None
