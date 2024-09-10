from datetime import datetime

import jsonpickle

from constants.idColumns import NO_ID
from datatypes.PatientAnonymizedIdentifier import PatientAnonymizedIdentifier
from datatypes.ResourceIdentifier import ResourceIdentifier
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime, is_not_nan


class Resource:
    def __init__(self, id_value: str, resource_type: str, counter: Counter, hospital_name: str):
        """

        :param id_value:
        :param resource_type:
        """
        self.identifier = None
        if id_value == NO_ID:
            # we are creating a new instance, we assign it a new ID
            # for Sample data only, we expect to have an ID assigned by the hospital, thus it cannot be NO_ID
            if resource_type == TableNames.SAMPLE:
                raise ValueError("Sample instances should have an ID.")
            else:
                id_to_use = str(counter.increment())
        else:
            # we are retrieving a resource from the DB and reconstruct it in-memory:
            # it already has an identifier, thus we simply reconstruct it with the value
            id_to_use = id_value

        # create the right Identifier based on the resource type:
        if resource_type == TableNames.PATIENT:
            # we use anonymized patient id
            if id_value == NO_ID:
                # we are creating a new Patient anonymized ID
                self.identifier = PatientAnonymizedIdentifier(id_value=id_to_use, hospital_name=hospital_name)
            else:
                # we are retrieving an existing anonymized patient ID
                self.identifier = PatientAnonymizedIdentifier(id_value=id_to_use, hospital_name=None)
        else:
            # We assign a "simple" (stringified integer) ID to the new resource
            self.identifier = ResourceIdentifier(id_value=id_to_use)

        self.resource_type = resource_type
        self.timestamp = get_mongodb_date_from_datetime(current_datetime=datetime.now())

    def get_identifier_as_int(self):
        # Resource identifiers (except Patient ones, which override this method) are a stringified int, e.g., "1", "2", etc
        # we only need to cast this value to int
        return self.identifier.get_as_int()

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
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
