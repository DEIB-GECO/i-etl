import jsonpickle


class Identifier:
    def __init__(self, value: str):
        self.value = value

    def get_int_id(self):
        raise NotImplemented("This method should be overridden in child classes.")

    def __getstate__(self):
        # we override this method to only return the actual Identifier value, and not a dict { "value": X }
        return self.value

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
