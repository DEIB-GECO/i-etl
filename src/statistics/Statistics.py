import jsonpickle


class Statistics:
    def __init__(self, record_stats: bool):
        self.record_stats = record_stats

    def __getstate__(self):
        # we want to add only dict and list members while serializing statistics
        # other members may be of type Database or Execution, and we do not want to serialize them as part of the stats
        state = self.__dict__.copy()
        # trick: we need to work on the copy of the keys to not directly work on them
        # otherwise, Concurrent modification error
        for key in list(state.keys()):
            if not isinstance(state[key], list) and not isinstance(state[key], dict):
                del state[key]
        return state

    def to_json(self):
        # encode creates a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))
