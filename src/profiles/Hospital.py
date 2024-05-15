import json

from src.database.TableNames import TableNames
from src.profiles.Resource import Resource
from src.utils.setup_logger import log


class Hospital(Resource):
	ID_COUNTER = 1

	def __init__(self, hospital_name: str):
		super().__init__()
		self.id = Hospital.ID_COUNTER
		Hospital.ID_COUNTER = Hospital.ID_COUNTER + 1
		self.url = "Hospital" + str(self.id)
		self.name = hospital_name

	# TODO Nelly: do this for each resource
	# def compute_unique_url(self):
	# 	return "%s/%d" % (TableNames.HOSPITAL_TABLE_NAME, self.id)

	def get_id(self):
		return self.id

	def get_url(self):
		return self.url

	def get_name(self):
		return self.name

	def to_json(self):
		json_hospital = {
			"id": str(self.id),
			"url": str(self.url),
			"name": str(self.name)
		}
		return json_hospital

	def get_resource_type(self):
		return TableNames.HOSPITAL_TABLE_NAME
