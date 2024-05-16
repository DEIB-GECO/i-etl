import math
import re

from src.utils.setup_logger import log


# ASSERTIONS

def is_float(value) -> bool:
    if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
        try:
            float(value)
            return True
        except ValueError:
            return False
    else:
        # for dict, lists, etc
        return False


def is_not_nan(value) -> bool:
    return not is_float(value) or (is_float(value) and not math.isnan(float(value)))


def assert_not_none(variable, variable_name: str) -> None:
    assert variable is not None, "The variable " + variable_name + " is None, while it should not."


def assert_type(variable, expected_type, variable_name: str) -> None:
    assert isinstance(variable_name, str)

    assert isinstance(variable, expected_type), "The variable " + variable_name + " is of type " + str(
        type(variable)) + " while it should be of type " + str(expected_type) + "."


def assert_regex(value: str, regex: str) -> None:
    assert_type(variable=value, expected_type=str, variable_name="value")
    assert_type(variable=regex, expected_type=str, variable_name="regex")

    # returns True if the string matches the given regex, False else
    assert bool(re.match(pattern=regex, string=value)) is True, "The value " + value + "does not match the regex '" + regex + "'."


# BUILDING URLs

def build_url(base: str, id: int) -> str:
    assert_type(base, str, "base")
    assert_type(id, int, "id")

    return base + "/" + str(id)
