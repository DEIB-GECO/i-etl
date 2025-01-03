from database.Counter import Counter
from datatypes.OntologyResource import OntologyResource
from entities.Feature import Feature
from enums.Profile import Profile
from enums.Visibility import Visibility


class DiagnosisFeature(Feature):
    def __init__(self, name: str, ontology_resource: OntologyResource, permitted_datatype: str, unit: str | None,
                 counter: Counter, categories: list[OntologyResource] | None,
                 visibility: Visibility, dataset_gid: str):
        """
        Create a new Disease instance.
        This is different from a DiseaseRecord:
        - a Disease instance models a disease definition
        - a DiseaseRecord instance models that Patient P has Disease D
        :param ontology_resource: the set of ontology terms (LOINC, ICD, ...) referring to that disease.
        """
        # set up the resource ID
        super().__init__(name=name, profile=Profile.DIAGNOSIS, ontology_resource=ontology_resource,
                         column_type=permitted_datatype, unit=unit, counter=counter,
                         categories=categories, visibility=visibility, dataset_gid=dataset_gid)
