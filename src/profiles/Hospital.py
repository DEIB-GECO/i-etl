import json

from src.database.TableNames import TableNames
from src.profiles.Resource import Resource
from src.utils.setup_logger import log


class Hospital(Resource):
	ID_COUNTER = 1

	def __init__(self, hospital_name: str):
		super().__init__()
		self.id = Hospital.ID_COUNTER
		self.name = hospital_name
		Hospital.ID_COUNTER = Hospital.ID_COUNTER + 1

	def compute_unique_url(self):
		return "%s/%d" % (TableNames.HOSPITAL_TABLE_NAME, self.id)

	def get_id(self):
		return self.id

	def get_name(self):
		return self.name

	def to_json(self):
		log.debug(json.dumps(self.__dict__))
		return json.loads(json.dumps(self.__dict__))

	def get_resource_type(self):
		return TableNames.HOSPITAL_TABLE_NAME

	def __str__(self):
		return "Hospital named " + self.name + " and of ID " + str(self.id)

	def __repr__(self):
		return "Hospital('"+str(self.id)+", "+self.name+")"
