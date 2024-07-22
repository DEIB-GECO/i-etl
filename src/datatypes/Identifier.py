import jsonpickle


class Identifier:
    def __init__(self, id_value: str, resource_type: str):
        self.value = id_value
    # def __init__(self, id_value: str, resource_type: str):
        # if not isinstance(id_value, str):
        #     # in case the dataframe has converted IDs to integers
        #     id_value = str(id_value)
        #
        # if "/" in id_value:
        #     # the provided id_value is already of the form type/id, thus we do not need to append the resource type
        #     # this happens when we build (instance) resources from the existing data in the database
        #     # the stored if is already of the form type/id
        #     self.value = id_value
        # else:
        #     self.value = resource_type + "/" + id_value

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
