from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.TableNames import TableNames


class Disease(Resource):
    def __init__(self, id_value: str, status: str, code: CodeableConcept):
        """
        Create a new Disease instance.
        This is different from a DiseaseRecord:
        - a Disease instance models a disease definition
        - a DiseaseRecord instance models that Patient P has Disease D

        :param status: a value in [draft, active, retired, unknown] to specify whether this disease definition can still be used.
        :param code: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=self.get_type())

        # set up the resource attributes
        self.status = status
        self.code = code

    def get_type(self):
        return TableNames.DISEASE.value

    def to_json(self):
        json_disease = {
            "identifier": self.identifier,
            "resourceType": self.get_type(),
            "status": self.status,
            "code": self.code.to_json()
        }
        return json_disease
