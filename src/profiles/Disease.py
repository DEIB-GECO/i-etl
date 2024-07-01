from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from profiles.Resource import Resource
from utils.Counter import Counter
from utils.TableNames import TableNames
from utils.utils import get_mongodb_date_from_datetime


class Disease(Resource):
    def __init__(self, id_value: str, status: str, code: CodeableConcept, counter: Counter):
        """
        Create a new Disease instance.
        This is different from a DiseaseRecord:
        - a Disease instance models a disease definition
        - a DiseaseRecord instance models that Patient P has Disease D

        :param status: a value in [draft, active, retired, unknown] to specify whether this disease definition can still be used.
        :param code: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type(), counter=counter)

        # set up the resource attributes
        self.code = code

    def get_type(self) -> str:
        return TableNames.DISEASE.value

    def to_json(self) -> dict:
        return {
            "identifier": self.identifier.to_json(),
            "resourceType": self.get_type(),
            "code": self.code.to_json(),
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
