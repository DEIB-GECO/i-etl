from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames
from src.utils.utils import NONE_VALUE


class Hospital(Resource):
    def __init__(self, id_value: str, name: str):
        """
        A new hospital instance, either built from existing data or from scratch.
        :param id_value: A stringified integer being the BETTER id of that resource.
        This is initially NONE_VALUE if we create a new Hospital from scratch.
        :param name: A string being the name of the hospital.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.name = name

    def get_type(self) -> str:
        """
        Get the resource type, i.e., Hospital.
        :return: A string being the resource type, i.e., Hospital.
        """
        return TableNames.HOSPITAL.value

    def to_json(self) -> dict:
        """
        Get the JSON representation of the resource.
        :return: A JSON dict being the Hospital with all its attributes.
        """
        json_hospital = {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "name": self.name
        }
        return json_hospital
