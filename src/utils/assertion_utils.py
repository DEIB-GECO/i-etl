import math
from typing import Any

from pandas import DataFrame

# moving it from constants.py to here
# otherwise it creates a circular dependency (and it is only used in this file)
THE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


# ASSERTIONS

def is_float(value: Any) -> bool:
    if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
        try:
            float(value)
            return True
        except ValueError:
            return False
    else:
        # for dict, lists, etc
        return False


def is_not_nan(value: Any) -> bool:
    return (not is_float(value=value) and value is not None) or (is_float(value=value) and not math.isnan(float(value)))


def is_not_empty(variable: Any) -> bool:
    if isinstance(variable, int) or isinstance(variable, float):
        return True
    elif isinstance(variable, str):
        return variable != ""
    elif isinstance(variable, list):
        return variable is not None and variable != []
    elif isinstance(variable, dict):
        return variable is not None and variable != {}
    elif isinstance(variable, tuple):
        return variable is not None and variable != ()
    elif isinstance(variable, DataFrame):
        return not variable.empty
    elif isinstance(variable, set):
        return variable != set()
    else:
        # no clue about the variable typ
        # thus, we only check whether it is None
        return variable is not None


def is_equal_insensitive(value: str | float, compared: str | float) -> bool:
    if not isinstance(value, str):
        return value == compared
    else:
        return value.casefold() == compared.casefold()

