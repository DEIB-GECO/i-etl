from datatypes.OntologyResource import OntologyResource
from enums.TableNames import TableNames
from enums.Visibility import Visibility
from entities.Feature import Feature
from database.Counter import Counter


class GenomicFeature(Feature):
    def __init__(self, id_value: str, original_name: str, ontology_resource: OntologyResource, permitted_datatype: str, dimension: str, counter: Counter,
                 hospital_name: str, categorical_values: list[OntologyResource], visibility: Visibility):
        # set up the resource ID
        super().__init__(id_value=id_value, original_name=original_name, ontology_resource=ontology_resource, column_type=permitted_datatype, dimension=dimension,
                         resource_type=TableNames.GENOMIC_FEATURE, counter=counter, hospital_name=hospital_name,
                         categorical_values=categorical_values, visibility=visibility)
