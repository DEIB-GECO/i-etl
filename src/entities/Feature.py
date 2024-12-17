import json

from constants.idColumns import NO_ID
from datatypes.OntologyResource import OntologyResource
from enums.TableNames import TableNames
from enums.Visibility import Visibility
from entities.Resource import Resource
from database.Counter import Counter
from types import SimpleNamespace


class Feature(Resource):
    def __init__(self, name: str, ontology_resource: OntologyResource, column_type: str, unit: str, counter: Counter,
                 profile: str, categories: list[OntologyResource], visibility: Visibility, dataset_gid: str):
        # set up the resource ID
        super().__init__(id_value=NO_ID, entity_type=f"{profile}{TableNames.FEATURE}", counter=counter)

        # set up the resource attributes
        self.name = name
        self.ontology_resource = ontology_resource
        self.data_type = column_type  # no need to check whether the column type is recognisable, we already normalized it while loading the metadata
        self.unit = unit
        if categories is not None and len(categories) > 0:
            self.categories = categories  # this is the list of categorical values fot that column
        else:
            self.categories = None  # this avoids to store empty arrays when there is no categorical values for a certain Feature
        self.visibility = visibility
        self.datasets = [dataset_gid]

    @classmethod
    def from_json(cls, json_feature: str):  # returns a specialized Feature instance, i.e., LabFeature, SampleFeature, etc.
        # fill a new XFeature instance with a JSON-encoded Feature
        return json.loads(json_feature, object_hook=lambda d: SimpleNamespace(**d))
        # if "resource_type" in feature_from_json:
        #     resource_type = feature_from_json["resource_type"]
        #     if resource_type == f"{Profile.PHENOTYPIC}{TableNames.FEATURE}":
        #         # return PhenotypicFeature(id_value=json_feature["identifier"], name=json_feature["name"],
        #         #                          ontology_resource=None, permitted_datatype=json_feature["dataType"],
        #         #                          unit=json_feature["unit"], counter=Counter(), hospital_name=hospital_name,
        #         #                          )
        #     elif resource_type == f"{Profile.CLINICAL}{TableNames.FEATURE}":
        # else:
        #     log.error("Unknown Feature type. Will reconstruct a PhenotypicFeature by default.")
