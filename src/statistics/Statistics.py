from datetime import datetime

import jsonpickle

from database.Database import Database
from database.Execution import Execution
from utils.setup_logger import log


class Statistics:
    def __init__(self, record_stats: bool):
        self.record_stats = record_stats
        self.timestamp = datetime.now()

    def __getstate__(self):
        # we want to add only dict and list members while serializing statistics
        # other members may be of type Database or Execution, and we do not want to serialize them as part of the stats
        state = self.__dict__.copy()
        # trick: we need to work on the copy of the keys to not directly work on them
        # otherwise, Concurrent modification error
        for key in list(state.keys()):
            if isinstance(state[key], Database) or isinstance(state[key], Execution):
                # we get rid of the variables that should not be included in the JSONification of Statistics objects
                # + mongodb does not allow to insert empty dicts or arrays
                del state[key]
        return state

    def to_json(self):
        # encode creates a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        # log.info(jsonpickle.decode(jsonpickle.encode(self, unpicklable=False)))
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
