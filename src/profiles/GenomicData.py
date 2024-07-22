from datetime import datetime

from datatypes.Reference import Reference
from profiles.Analysis import Analysis
from profiles.Resource import Resource
from enums.TableNames import TableNames
from utils.Counter import Counter
from utils.utils import get_mongodb_date_from_datetime


class GenomicData(Resource):
    def __init__(self, id_value: str, analysis: Analysis, subject_ref: Reference, hospital_ref: Reference, counter: Counter):
        super().__init__(id_value=id_value, resource_type=TableNames.GENOMIC_DATA.value, counter=counter)

        self.analysis = analysis
        self.subject = subject_ref
        self.recorded_by = hospital_ref
