from database.Counter import Counter
from datatypes.OntologyResource import OntologyResource
from entities.Feature import Feature
from enums.Profile import Profile
from enums.Visibility import Visibility


class MedicineFeature(Feature):
    def __init__(self, name: str, ontology_resource: OntologyResource, permitted_datatype: str, unit: str, counter: Counter,
                 categories: list[OntologyResource], visibility: Visibility, dataset_gid: str):
        super().__init__(name=name, ontology_resource=ontology_resource, column_type=permitted_datatype, unit=unit,
                         profile=Profile.MEDICINE, counter=counter, categories=categories, visibility=visibility, dataset_gid=dataset_gid)
