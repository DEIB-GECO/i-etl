from datetime import datetime

from datatypes.CodeableConcept import CodeableConcept
from profiles.Feature import Feature
from profiles.Resource import Resource
from utils.Counter import Counter
from enums.TableNames import TableNames
from utils.utils import get_mongodb_date_from_datetime


class DiagnosisFeature(Feature):
    def __init__(self, id_value: str, code: CodeableConcept, permitted_datatype: str, dimension: str, counter: Counter):
        """
        Create a new Disease instance.
        This is different from a DiseaseRecord:
        - a Disease instance models a disease definition
        - a DiseaseRecord instance models that Patient P has Disease D
        :param code: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, resource_type=TableNames.DIAGNOSIS_FEATURE, column_type=permitted_datatype, dimension=dimension, counter=counter)

        # set up the resource attributes
        self.code = code
