import json


class Resource:

    def __init__(self):
        pass

    def get_resource_type(self):
        raise NotImplementedError("The method get_resource_type() has to be overridden in every child class.")

    def to_json(self):
        raise NotImplementedError("The method to_json() has to be overridden in every child class.")

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())
