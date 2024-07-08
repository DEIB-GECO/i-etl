class Identifier:
    def __init__(self, id_value: int, resource_type: str):
        self._value = id_value
    # def __init__(self, id_value: str, resource_type: str):
        # if not isinstance(id_value, str):
        #     # in case the dataframe has converted IDs to integers
        #     id_value = str(id_value)
        #
        # if "/" in id_value:
        #     # the provided id_value is already of the form type/id, thus we do not need to append the resource type
        #     # this happens when we build (instance) resources from the existing data in the database
        #     # the stored if is already of the form type/id
        #     self._value = id_value
        # else:
        #     self._value = resource_type + "/" + id_value

    @property
    def value(self) -> int:
        return self._value

    # @property
    # def value(self) -> str:
    #     return self._value

    def to_json(self) -> dict:
        return {
            "value": self._value
        }
