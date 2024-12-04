from datatypes.OntologyResource import OntologyResource
from enums.TableNames import TableNames
from enums.Visibility import Visibility
from entities.Feature import Feature
from database.Counter import Counter


class DiagnosisFeature(Feature):
    def __init__(self, id_value: str, name: str, ontology_resource: OntologyResource, permitted_datatype: str, unit: str | None,
                 counter: Counter, hospital_name: str, categories: list[OntologyResource] | None,
                 visibility: Visibility):
        """
        Create a new Disease instance.
        This is different from a DiseaseRecord:
        - a Disease instance models a disease definition
        - a DiseaseRecord instance models that Patient P has Disease D
        :param ontology_resource: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
        """
        # set up the resource ID
        super().__init__(id_value=id_value, name=name, resource_type=TableNames.DIAGNOSIS_FEATURE, ontology_resource=ontology_resource,
                         column_type=permitted_datatype, unit=unit, counter=counter,
                         hospital_name=hospital_name, categories=categories, visibility=visibility)
