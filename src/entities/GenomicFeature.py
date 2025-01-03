from database.Counter import Counter
from datatypes.OntologyResource import OntologyResource
from entities.Feature import Feature
from enums.Profile import Profile
from enums.Visibility import Visibility


class GenomicFeature(Feature):
    def __init__(self, name: str, ontology_resource: OntologyResource, permitted_datatype: str, unit: str, counter: Counter,
                 description: str, categories: list[OntologyResource], visibility: Visibility, dataset_gid: str):
        # set up the resource ID
        super().__init__(name=name, ontology_resource=ontology_resource, column_type=permitted_datatype, unit=unit, description=description,
                         profile=Profile.GENOMIC, counter=counter, categories=categories, visibility=visibility, dataset_gid=dataset_gid)
