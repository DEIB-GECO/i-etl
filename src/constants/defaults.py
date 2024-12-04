import re

import requests

API_SESSION = requests.Session()

# 1000 is the max supported by Mongodb.
# If we set BATCH_SIZE > 1000, Mongodb will send batch of 1k instances,
# but we will still have fewer files to write and read, thus increasing performance
BATCH_SIZE = 10000

PATTERN_VALUE_UNIT = re.compile(r'^ *([0-9]+[.,]*[0-9]*) *([a-zA-Z_.-]+) *$')  # we add start and end delimiters (^ and $) to not process cells with multiples values inside

SNOMED_OPERATORS_LIST = ["|", "(", ")", "{", "}", ",", ":", "=", "+"]
SNOMED_OPERATORS_STR = "".join(SNOMED_OPERATORS_LIST)

DEFAULT_ONTOLOGY_RESOURCE_LABEL = ""
