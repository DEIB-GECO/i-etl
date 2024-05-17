from src.profiles.Resource import Resource
from src.utils.utils import build_url, HOSPITAL_TABLE_NAME


class Hospital(Resource):
	ID_COUNTER = 1

	def __init__(self, hospital_name: str):
		super().__init__()
		self.id = Hospital.ID_COUNTER
		Hospital.ID_COUNTER = Hospital.ID_COUNTER + 1
		self.url = build_url(HOSPITAL_TABLE_NAME, self.id)
		self.name = hospital_name

	def get_url(self) -> str:
		return self.url

	def get_resource_type(self) -> str:
		return HOSPITAL_TABLE_NAME

	def to_json(self) -> dict:
		json_hospital = {
			"id": self.id,
			"url": self.url,
			"name": self.name
		}
		return json_hospital
