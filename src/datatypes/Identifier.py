class Identifier:
    def __init__(self, value: str):
        self.value = value

    def to_json(self):
        return {
            "value": self.value
        }
    