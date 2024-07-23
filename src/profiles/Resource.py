import json
from datetime import datetime

import jsonpickle

from datatypes.Identifier import Identifier
from enums.TableNames import TableNames
from utils import constants
from utils.Counter import Counter
from utils.setup_logger import log
from utils.utils import get_mongodb_date_from_datetime, is_not_nan


class Resource:
    def __init__(self, id_value: str, resource_type: str, counter: Counter):
        """

        :param id_value:
        :param resource_type:
        """
        self.identifier = None  # change the FHIR model to have an identifier which is simply a string
        if id_value == constants.NO_ID:
            if resource_type == TableNames.PATIENT or resource_type == TableNames.SAMPLE:
                # Patient instances should always have an ID (given by the hospitals)
                # this may contain chars, so we need to keep them as strings
                raise ValueError("Patient and Sample instances should have an ID.")
            else:
                # We assign an ID to the new resource
                self.identifier = Identifier(id_value=str(counter.increment()), resource_type=resource_type)
        else:
            # This case covers when we retrieve resources from the DB, and we reconstruct them in-memory:
            # they already have an identifier, thus we simply reconstruct it with the value
            self.identifier = Identifier(id_value=id_value, resource_type=resource_type)

        self.resource_type = resource_type
        self.timestamp = get_mongodb_date_from_datetime(current_datetime=datetime.now())

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
                    state[key] = get_mongodb_date_from_datetime(state[key])
        return state

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        log.debug(jsonpickle.decode(jsonpickle.encode(self, unpicklable=False)))
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
