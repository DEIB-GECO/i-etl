class Identifier:
    def __init__(self, id_value: str, resource_type: str):
        if not isinstance(id_value, str):
            # in case the dataframe has converted IDs to integers
            id_value = str(id_value)

        if "/" in id_value:
            # the provided id_value is already of the form type/id, thus we do not need to append the resource type
            # this happens when we build (instance) resources from the existing data in the database
            # the stored if is already of the form type/id
            self.value = id_value
        else:
            self.value = resource_type + "/" + id_value

    def to_json(self):
        return {
            "value": self.value
        }
