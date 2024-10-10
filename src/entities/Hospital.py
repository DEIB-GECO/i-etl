from enums.TableNames import TableNames
from entities.Resource import Resource
from database.Counter import Counter


class Hospital(Resource):
    def __init__(self, id_value: str, name: str, counter: Counter):
        """
        A new hospital instance, either built from existing data or from scratch.
        :param id_value: A stringified integer being the BETTER id of that resource.
        This is initially NO_ID if we create a new Hospital from scratch.
        :param name: A string being the name of the hospital.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=TableNames.HOSPITAL, counter=counter, hospital_name=name)

        # set up the resource attributes
        self.name = name
