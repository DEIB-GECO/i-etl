from src.database.TableNames import TableNames
from src.profiles.Resource import Resource


class Disease(Resource):
	def __init__(self, url: str,status: str, code: CodeableConcept):
		"""
		Create a new Disease instance.
		This is different from a DiseaseRecord:
		- a Disease instance models a disease definition
		- a DiseaseRecord instance models that Patient P has Disease D

		:param url:  the (unique) uri to identify this disease in the project.
		:param status: a value in [draft, active, retired, unknown] to specify whether this disease definition can still be used.
		:param code: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
		"""
		super().__init__()
		self.url = url
		self.status = status
		self.code = code

	def get_resource_type(self):
		return TableNames.DISEASE_TABLE_NAME

	def to_json(self):
		json_disease = {
			"url": self.url,
			"status": self.status,
			"code": self.code
		}
		return json_disease