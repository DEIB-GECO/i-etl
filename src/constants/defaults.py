import re


BATCH_SIZE = 1000

PATTERN_VALUE_DIMENSION = re.compile(r'^ *([0-9]+[.,]*[0-9]*) *([a-zA-Z_.-]+) *$')  # we add start and end delimiters (^ and $) to not process cells with multiples values inside

SNOMED_OPERATORS_LIST = ["|", "(", ")", "{", "}", ",", ":", "=", "+"]
SNOMED_OPERATORS_STR = "".join(SNOMED_OPERATORS_LIST)

DEFAULT_ONTOLOGY_RESOURCE_LABEL = ""
