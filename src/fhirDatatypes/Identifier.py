import json


class Identifier:
    def __init__(self, id_value: str, use: str):
        self.value = id_value
        self.use = use  # 'official' for IDs given by hospitals, 'secondary' for the BETTER IDs

    def to_json(self):
        json_identifier = {
            "value": self.value,
            "use": self.use,
        }
        return json_identifier

    def __str__(self):
        return json.dumps(self.to_json())

    def __repr__(self):
        return json.dumps(self.to_json())

    def __eq__(self, other) -> bool:
        if isinstance(other, Identifier):
            return self.value == other.value
        return False

    def __hash__(self):
        return hash(self.value)
