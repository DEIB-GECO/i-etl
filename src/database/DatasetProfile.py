from datetime import datetime

import jsonpickle

from database.Database import Database
from database.Operators import Operators
from utils.assertion_utils import is_not_nan


class DatasetProfile:
    def __init__(self, description: str, theme: str, filetype: str, size: int, nb_tuples: int, completeness: int, uniqueness: float):
        self.description = description
        self.theme = theme
        self.filetype = filetype
        self.size = size
        self.nb_tuples = nb_tuples
        self.completeness = completeness
        self.uniqueness = uniqueness

    def __getstate__(self):
        # we need to check whether each field is a NaN value because we do not want to add fields for NaN values
        state = self.__dict__.copy()
        # trick: we need to work on the copy of the keys to not directly work on them
        # otherwise, Concurrent modification error
        for key in list(state.keys()):
            if state[key] is None or not is_not_nan(state[key]) or type(state[key]) is Database:
                del state[key]
            else:
                # we may also need to convert datetime within MongoDB-style dates
                if isinstance(state[key], datetime):
                    state[key] = Operators.from_datetime_to_isodate(state[key])
        return state

    def to_json(self):
        # encode creates a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
