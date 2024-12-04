from datatypes.OntologyResource import OntologyResource
from enums.TableNames import TableNames
from enums.Visibility import Visibility
from entities.Feature import Feature
from database.Counter import Counter


class GenomicFeature(Feature):
    def __init__(self, id_value: str, name: str, ontology_resource: OntologyResource, permitted_datatype: str, unit: str, counter: Counter,
                 hospital_name: str, categories: list[OntologyResource], visibility: Visibility):
        # set up the resource ID
        super().__init__(id_value=id_value, name=name, ontology_resource=ontology_resource, column_type=permitted_datatype, unit=unit,
                         resource_type=TableNames.GENOMIC_FEATURE, counter=counter, hospital_name=hospital_name,
                         categories=categories, visibility=visibility)
