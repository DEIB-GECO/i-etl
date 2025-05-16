import re
from datetime import datetime

import numpy as np
import requests


API_SESSION = requests.Session()

# 1000 is the max supported by Mongodb.
# If we set BATCH_SIZE > 1000, Mongodb will send batch of 1k instances,
# but we will still have fewer files to write and read, thus increasing performance
BATCH_SIZE = 10000
MAX_FILE_SIZE = 15 * 1048576  # 10Mb = 10x 1024^2 (bytes) -- MongoDB is limited to 16Mo but our size estimate is usually lower than the real size so we stay at 10Mo to avoid creating files with a real size larger than 16Mo

PATTERN_VALUE_UNIT = re.compile(r'^ *([0-9]+[.,]*[0-9]*) *([a-zA-Z_.-]+) *$')  # we add start and end delimiters (^ and $) to not process cells with multiples values inside

SNOMED_OPERATORS_LIST = ["|", "(", ")", "{", "}", ",", ":", "=", "+"]
SNOMED_OPERATORS_STR = "".join(SNOMED_OPERATORS_LIST)

DEFAULT_ONTOLOGY_RESOURCE_LABEL = ""

DATASET_GLOBAL_IDENTIFIER_PREFIX = "http://better-health-project.eu/datasets/"

NO_ID = -1

NAN_VALUES = ["no information", "-", "nan", "na", "none", "n/a", "null", "np.nan", np.nan, "/"]
DEFAULT_NAN_VALUE = np.nan

PRINT_QUERIES = True
