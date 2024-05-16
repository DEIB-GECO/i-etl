from src.database.TableNames import TableNames
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Resource import Resource
from src.utils.utils import build_url


class Disease(Resource):
	ID_COUNTER = 1

	def __init__(self, status: str, code: CodeableConcept):
		"""
		Create a new Disease instance.
		This is different from a DiseaseRecord:
		- a Disease instance models a disease definition
		- a DiseaseRecord instance models that Patient P has Disease D

		:param status: a value in [draft, active, retired, unknown] to specify whether this disease definition can still be used.
		:param code: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
		"""
		super().__init__()
		self.id = Disease.ID_COUNTER
		Disease.ID_COUNTER = Disease.ID_COUNTER + 1
		self.url = build_url(TableNames.DISEASE_TABLE_NAME, self.id)
		self.status = status
		self.code = code

	def get_url(self):
		return self.url

	def get_resource_type(self):
		return TableNames.DISEASE_TABLE_NAME

	def to_json(self):
		json_disease = {
			"id": self.id,
			"url": self.url,
			"status": self.status,
			"code": self.code.to_json()
		}

		return json_disease
