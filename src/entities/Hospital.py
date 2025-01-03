from constants.defaults import NO_ID
from enums.TableNames import TableNames
from entities.Resource import Resource
from database.Counter import Counter


class Hospital(Resource):
    def __init__(self, name: str, counter: Counter):
        """
        A new hospital instance, either built from existing data or from scratch.
        :param name: A string being the name of the hospital.
        """
        # set up the resource ID
        super().__init__(id_value=NO_ID, entity_type=TableNames.HOSPITAL, counter=counter)

        # set up the resource attributes
        self.name = name
