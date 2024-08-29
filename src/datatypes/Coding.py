from urllib.parse import quote

import jsonpickle

from enums.AccessTypes import AccessTypes
from enums.Ontologies import Ontologies
from utils.constants import DEFAULT_CODING_DISPLAY
from utils.setup_logger import log
from utils.utils import is_not_nan, normalize_ontology_code, parse_xml_response, parse_json_response, send_query_to_api


class Coding:
    def __init__(self, ontology: Ontologies, code: str, display: str|None):
        if not is_not_nan(value=ontology):
            # no ontology code has been provided for that variable name, let's skip it
            log.error("Could not create a Coding with no ontology resource.")
            self.system = None
            self.code = None
            self.display = None
        else:
            self.system = ontology["url"]  # this is the ontology url, not the ontology name
            self.code = normalize_ontology_code(ontology_code=code)
            if display is None:
                # when we create a new CodeableConcept from scratch, we need to compute the display with ontology API
                # if the query to the API does not work, we can still use the column name as the display of the coding
                # the column name is stored in display to avoid adding another parameter to the Coding constructor
                self.display = Coding.compute_display_from_api(ontology=ontology, ontology_code=self.code)
            else:
                # when we retrieve a CodeableConcept from the db, we do NOT want to compute again its display
                # we still get the existing display
                self.display = display
            # log.debug(f"Create a new Coding for {self.system}/{self.code}, labelled {self.display}")

    @classmethod
    def compute_display_from_api(cls, ontology: Ontologies, ontology_code: str) -> str:
        # column_name is to be used when the display of the Coding could not be computed with any of the APIs
        compute_from_api = True
        if compute_from_api:
            try:
                if ontology == Ontologies.SNOMEDCT:
                    url_resource = quote(f"http://purl.bioontology.org/ontology/SNOMEDCT/{ontology_code}", safe="")
                    url = f"http://data.bioontology.org/ontologies/SNOMEDCT/classes/{url_resource}"
                    response = send_query_to_api(url=url, secret="d6fb9c05-3309-4158-892f-65434a9133b9", access_type=AccessTypes.API_KEY_IN_URL)
                    data = parse_json_response(response)
                    if "prefLabel" in data:
                        return data["prefLabel"]
                    else:
                        return DEFAULT_CODING_DISPLAY
                elif ontology == Ontologies.LOINC:
                    url = f"https://loinc.regenstrief.org/searchapi/loincs?query={ontology_code}"
                    response = send_query_to_api(url=url, secret="nbarret d7=47@xiz$g=-Ns", access_type=AccessTypes.AUTHENTICATION)
                    data = parse_json_response(response)
                    if "Results" in data and len(data["Results"]) > 0 and "COMPONENT" in data["Results"][0]:
                        return data["Results"][0]["COMPONENT"]
                    else:
                        return DEFAULT_CODING_DISPLAY
                elif ontology == Ontologies.PUBCHEM:
                    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{ontology_code}/description/JSON"
                    response = send_query_to_api(url, secret=None, access_type=AccessTypes.USER_AGENT)
                    data = parse_json_response(response)
                    if ("InformationList" in data
                            and "Information" in data["InformationList"]
                            and len(data["InformationList"]["Information"]) > 0
                            and "Title" in data["InformationList"]["Information"][0]):
                        return data["InformationList"]["Information"][0]["Title"]
                    else:
                        return DEFAULT_CODING_DISPLAY
                elif ontology == Ontologies.CLIR:
                    # TODO Nelly: code this
                    return DEFAULT_CODING_DISPLAY
                elif ontology == Ontologies.GSSO:
                    iri = f"http://purl.obolibrary.org/obo/{ontology_code}"
                    url = f"https://ontobee.org/ontology/GSSO?iri={iri}"
                    response = send_query_to_api(url=url, secret="d6fb9c05-3309-4158-892f-65434a9133b9", access_type=AccessTypes.API_KEY_IN_BEARER)
                    data = parse_xml_response(response)  # data is an XML document
                    classes = data.getElementsByTagName('Class')
                    for one_class in classes:
                        if one_class.getAttribute("rdf:about") == iri:
                            if len(one_class.getElementsByTagName("rdfs:label")) > 0:
                                return one_class.getElementsByTagName("rdfs:label")[0].childNodes[0].data
                    return DEFAULT_CODING_DISPLAY
                elif ontology == Ontologies.ORPHANET:
                    ontology_code_without_prefix = ontology_code.replace("ORPHA:", "").replace("orpha:", "")
                    url = f"https://api.orphacode.org/EN/ClinicalEntity/orphacode/{ontology_code_without_prefix}/Name"
                    response = send_query_to_api(url=url, secret="nbarret", access_type=AccessTypes.API_KEY_IN_HEADER)
                    data = parse_json_response(response)
                    if "Preferred term" in data:
                        return data["Preferred term"]
                    else:
                        return DEFAULT_CODING_DISPLAY
                else:
                    # raise NotImplementedError("Not implemented yet.")
                    return DEFAULT_CODING_DISPLAY
            except Exception:
                # the API could not be queried, returning empty string.
                return DEFAULT_CODING_DISPLAY
        else:
            return DEFAULT_CODING_DISPLAY

    def to_json(self):
        # encode create a stringified JSON object of the class
        # and decode transforms the stringified JSON to a "real" JSON object
        return jsonpickle.decode(jsonpickle.encode(self, unpicklable=False))

    def __eq__(self, other):
        if not isinstance(other, Coding):
            raise TypeError(f"Could not compare the current instance with an instance of type {type(other)}.")

        # we do not use the display  because this would lead to unequal instances
        # if provided descriptions differ from one hospital to another
        return self.system == other.system and self.code == other.code

    # def __hash__(self):
    #     # we do not use the display in the hash
    #     # because this would lead to different hashes
    #     # if provided descriptions differ from one hospital to another
    #     my_hash = hash((self.system, self.code))
    #     log.debug(f"compute hash of {self.system} and {self.code}: {my_hash}")
    #     return my_hash

    def __str__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def __repr__(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
