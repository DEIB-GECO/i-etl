from datetime import datetime

import jsonpickle

from constants.idColumns import NO_ID
from database.Counter import Counter
from datatypes.Identifier import Identifier
from database.Operators import Operators
from utils.assertion_utils import is_not_nan


class Resource:
    def __init__(self, id_value: int, entity_type: str, counter: Counter):
        """

        :param id_value:
        :param entity_type:
        """
        self.identifier = None
        if id_value == NO_ID:
            # we are creating a new instance, we assign it a new ID
            id_to_use = counter.increment()
        else:
            # we are retrieving a resource from the DB and reconstruct it in-memory:
            # it already has an identifier, thus we simply reconstruct it with the value
            id_to_use = id_value

        self.identifier = Identifier(id_value=id_to_use)
        self.timestamp = Operators.from_datetime_to_isodate(current_datetime=datetime.now())
        self.entity_type = entity_type

    def __getstate__(self):
        # we need to check whether each field is a NaN value because we do not want to add fields for NaN values
        state = self.__dict__.copy()
        # trick: we need to work on the copy of the keys to not directly work on them
        # otherwise, Concurrent modification error
        for key in list(state.keys()):
            if state[key] is None or not is_not_nan(state[key]):
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
