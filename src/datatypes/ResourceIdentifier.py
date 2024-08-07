from datatypes.Identifier import Identifier


class ResourceIdentifier(Identifier):
    def __init__(self, id_value: str):
        super().__init__(value=id_value)

    def get_as_int(self):
        # Resource identifiers (except Patient ones, which override this method) are a stringified int, e.g., "1", "2", etc
        # we only need to cast this value to int
        return int(self.value)
