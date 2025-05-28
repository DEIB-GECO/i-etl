import dataclasses
import re
from urllib.parse import quote

from constants.defaults import SNOMED_OPERATORS_LIST, DEFAULT_ONTOLOGY_RESOURCE_LABEL, SNOMED_OPERATORS_STR
from constants.methods import factory
from enums.AccessTypes import AccessTypes
from enums.Ontologies import Ontologies
from statistics.QualityStatistics import QualityStatistics
from utils.api_utils import send_query_to_api, parse_xml_response, parse_json_response, parse_html_response
from utils.setup_logger import log
from utils.str_utils import remove_specific_tokens, process_spaces, remove_operators_in_strings


@dataclasses.dataclass(kw_only=True)
class OntologyResource:
    system: dict
    code: str
    label: str | None
    quality_stats: QualityStatistics | None = dataclasses.field(repr=False)

    # keys to be used when writing JSON or queries
    # those names have to exactly match the variables names declared in entity classes
    SYSTEM_ = "system"
    CODE_ = "code"
    LABEL_ = "label"

    def __post_init__(self):
        # every attribute that is there will be serialized in the Ontology Resource
        # to avoid this, one needs to explicitly say which attributes are to be removed from the JSON serialization
        # using the __get_state__ method
        self.quality_stats = self.quality_stats if self.quality_stats is not None else QualityStatistics(record_stats=False)
        if len(self.system) == 0 or len(self.code) == 0:
            # no ontology code has been provided for that variable name, let's skip it
            # the only case when we don't want to skip it is when a label is provided as input
            if self.label is not None and self.label != "":
                log.warning(f"Creating an OntologyResource with a label only (label={self.label}).")
            else:
                log.warning("Could not create an OntologyResource with no ontology system and/or code.")
        else:
            # this corresponds to the first (and only) ontology system;
            # if there are many, we record only the first but make API calls with all
            # this is because an Ontology can have a single system
            # TODO Nelly: maybe there is a better way?
            self.system = self.system["url"]
            self.code = self.code.replace("ORPHA:", "").replace("orpha:", "").replace("GO:", "").replace("go:", "")
            self.code = process_spaces(input_string=self.code)
            self.code = re.sub(r" *([" + SNOMED_OPERATORS_STR + "]+) *", r"\1", self.code)  # remove spaces around operators; r"\1" means: replace with first captured group
            self.code = remove_operators_in_strings(input_string=self.code)  # for every label inside |, '' or "", we remove possible operators
            code_elements = self.compute_elements(full_code=self.code)
            self.compute_code(code_elements=code_elements)
            if self.label is None:
                # when we create a new OntologyResource from scratch, we need to compute the label with ontology API
                # if the query to the API does not work, we can still use the column name as the label of the OntoResource
                self.compute_label(code_elements=code_elements, quality_stats=self.quality_stats)

    def compute_elements(self, full_code: str) -> list:
        elements = []
        regex_elements = re.split(r"(?=["+SNOMED_OPERATORS_STR+"])|(?<=["+SNOMED_OPERATORS_STR+"])", full_code)
        # now self.elements may still contain spaces
        # therefore, we remove them manually afterward
        for element in regex_elements:
            if element == "" or element is None or element == " ":
                # the regex sometimes returns empty or None elements, we skip them
                # it may also identify spaces around operators, we skip them too
                pass
            else:
                elements.append(element)
        return elements

    def compute_code(self, code_elements: list) -> None:
        self.code = ""
        for i in range(len(code_elements)):
            element = code_elements[i]
            if element not in SNOMED_OPERATORS_LIST:
                # this is not an operator
                if i-1 >= 0 and code_elements[i-1] == "|":
                    # we are in an annotation, thus we skip it
                    pass
                elif element.startswith("\""):
                    # this is a constant, thus we keep it and only process space (not caps)
                    self.code += process_spaces(input_string=element)
                else:
                    # this is a code
                    self.code += Ontologies.normalize_code(ontology_code=element)
            else:
                # this is an operator, we add it only if this in not the pipe
                if element != "|":
                    self.code += element

    def compute_label(self, code_elements: list, quality_stats: QualityStatistics) -> None:
        self.label = ""
        for i in range(len(code_elements)):
            element = code_elements[i]
            if element not in SNOMED_OPERATORS_LIST and not element.startswith("\""):  # " is used for surrounding constants, e.g., "HPO" in 278201002|Classification|="HPO"
                # this is not an operator, nor a constant
                if i - 1 >= 0 and code_elements[i - 1] == "|":
                    # we are in an annotation, thus we skip it
                    pass
                else:
                    # this is a code, we get its label (name)
                    resource_label = OntologyResource.get_resource_label_from_api(system=self.system, single_code=element, quality_stats=quality_stats)
                    if resource_label is not None:
                        resource_label = process_spaces(input_string=resource_label)
                        resource_label = remove_specific_tokens(input_string=resource_label, tokens=["(property)", "- finding", "-finding", "(qualifier value)", "(observable entity)", "(social concept)", "(procedure)", "(assessment scale)", "- action", "-action", "- attribute", "-attribute"])  # useless and may break parsing (due to parenthesis and dash)
                        resource_label = remove_specific_tokens(input_string=resource_label, tokens=SNOMED_OPERATORS_LIST)  # if we don't remove them, this will break future parsing
                        resource_label = process_spaces(input_string=resource_label)
                        self.label += resource_label
            else:
                # this is an operator, we add it only if this in not the pipe
                if element != "|":
                    self.label += element

    @classmethod
    def get_resource_label_from_api(cls, system: str, single_code: str, quality_stats: QualityStatistics) -> str:
        # column_name is to be used when the label of the OntologyResource could not be computed with any of the APIs
        compute_from_api = True
        if compute_from_api:
            try:
                if system == Ontologies.SNOMEDCT["url"]:
                    url_resource = quote(f"http://purl.bioontology.org/ontology/SNOMEDCT/{single_code}", safe="")
                    url = f"http://data.bioontology.org/ontologies/SNOMEDCT/classes/{url_resource}"
                    response = send_query_to_api(url=url, secret="d6fb9c05-3309-4158-892f-65434a9133b9", access_type=AccessTypes.API_KEY_IN_URL)
                    if response is None:
                        error = f"Failed connection to SNOMED-CT API."
                    elif response.status_code == 200:
                        data = parse_json_response(response)
                        if "prefLabel" in data:
                            return data["prefLabel"]
                        else:
                            error = f"No label field for resource {single_code}."
                    elif response.status_code == 404 or response.status_code == 400:
                        error = f"Resource {single_code} not found."
                    else:
                        error = f"Failed connection to SNOMED-CT API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.LOINC["url"]:
                    url = f"https://loinc.regenstrief.org/searchapi/loincs?query={single_code}"
                    response = send_query_to_api(url=url, secret="nbarret d7=47@xiz$g=-Ns", access_type=AccessTypes.AUTHENTICATION)
                    data = parse_json_response(response)
                    if response is None:
                        error = f"Failed connection to LOINC API."
                    elif "Results" in data:
                        if len(data["Results"]) > 0:
                            if "COMPONENT" in data["Results"][0]:
                                return data["Results"][0]["COMPONENT"]
                            else:
                                error = f"No field label for resource {single_code}"
                        else:
                            error = f"Resource {single_code} not found."
                    else:
                        error = f"Failed connection to LOINC API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.PUBCHEM["url"]:
                    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{single_code}/description/JSON"
                    response = send_query_to_api(url, secret=None, access_type=AccessTypes.USER_AGENT)
                    if response is None:
                        error = f"Failed connection to PUBCHEM API."
                    elif response.status_code == 404 or response.status_code == 400:
                        error = f"Resource {single_code} not found."
                    elif response.status_code == 200:
                        data = parse_json_response(response)
                        if "InformationList" in data and "Information" in data["InformationList"] and len(data["InformationList"]["Information"]) > 0:
                            if "Title" in data["InformationList"]["Information"][0]:
                                return data["InformationList"]["Information"][0]["Title"]
                            else:
                                error = f"No label field for resource {single_code}."
                        else:
                            error = f"Resource {single_code} not found."
                    else:
                        error = f"Failed connection to PUBCHEM API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.CLIR["url"]:
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error="No API access for the CLIR ontology.")
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.GSSO["url"]:
                    iri = f"http://purl.obolibrary.org/obo/{single_code.upper()}"  # we need to upper case the GSSO_, otherwise the API returns None
                    url = f"https://ontobee.org/ontology/GSSO?iri={iri}"
                    response = send_query_to_api(url=url, secret="d6fb9c05-3309-4158-892f-65434a9133b9", access_type=AccessTypes.API_KEY_IN_BEARER)
                    if response is None:
                        error = f"Failed connection to GSSO API."
                    elif response.status_code == 200:
                        data = parse_xml_response(response)  # data is an XML document
                        classes = data.getElementsByTagName('Class')
                        # <Class rdf:about="http://purl.obolibrary.org/obo/GSSO_006450">
                        #   <rdfs:label xml:lang="en">individual bullying</rdfs:label>
                        #   <rdfs:subClassOf rdf:resource="http://purl.obolibrary.org/obo/GSSO_000435" />
                        #   <ns2:IAO_0000115 xml:lang="en">Bullying perpetrated by a single person against a target ot targets.</ns2:IAO_0000115>
                        # </Class>
                        if len(classes) > 0:
                            expected_element = None
                            for one_class in classes:
                                if one_class.getAttribute("rdf:about") == iri:
                                    expected_element = one_class
                            if expected_element is None:
                                error = f"Resource {single_code} not found."
                            else:
                                if len(expected_element.getElementsByTagName("rdfs:label")) > 0:
                                    return expected_element.getElementsByTagName("rdfs:label")[0].childNodes[0].data
                                else:
                                    error = f"No field label for resource {single_code}"
                        else:
                            error = f"Resource {single_code} not found."
                    elif response.status_code == 404 or response.status_code == 400:
                        error = f"Resource {single_code} not found."
                    else:
                        error = f"Failed connection to GSSO API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.ORPHANET["url"]:
                    url = f"https://api.orphacode.org/EN/ClinicalEntity/orphacode/{single_code}/Name"
                    response = send_query_to_api(url=url, secret="nbarret", access_type=AccessTypes.API_KEY_IN_HEADER)
                    if response is None:
                        error = f"Failed connection to ORPHANET API."
                    elif response.status_code == 404 or response.status_code == 400:
                        error = f"Resource {single_code} not found."
                    elif response.status_code == 200:
                        data = parse_json_response(response)
                        if "Preferred term" in data:
                            return data["Preferred term"]
                        else:
                            error = f"No label field for resource {single_code}."
                    else:
                        error = f"Failed connection to ORPHANET API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.GENE_ONTOLOGY["url"]:
                    # as of 03/09/2024, this ontology is queried by accessing the webpage describing the resource
                    # it seems that there is an RDF query tool, but it is not sure that this can be queried as an API
                    # and there is no documentation on existing properties to query some codes
                    url = f"https://amigo.geneontology.org/amigo/term/GO:{single_code}"
                    response = send_query_to_api(url=url, secret=None, access_type=AccessTypes.USER_AGENT)
                    if response is None:
                        error = f"Failed connection to GO API."
                    elif response.status_code == 200:
                        data = parse_html_response(response)
                        header = data.select_one("div.page-header > h1").text
                        if header != "":
                            return header
                        else:
                            error = f"No label field for resource {single_code}."
                    elif response.status_code == 404 or response.status_code == 400:
                        error = f"Resource {single_code} not found."
                    else:
                        error = f"Failed connection to GO API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.OMIM["url"]:
                    # TODO NELLY: get OMIM API key (default one on OMIM website nfNEOscLNWWXdSmUoMLPPA is unauthorized)
                    url = f"https://api.omim.org/api/entry?mimNumber={single_code}&include=text&format=json"
                    response = send_query_to_api(url=url, secret="nfNEOscLNWWXdSmUoMLPPA", access_type=AccessTypes.API_KEY_IN_HEADER)
                    if response is None:
                        error = f"Failed connection to OMIM API."
                    elif response.status_code == 200:
                        data = parse_json_response(response)
                        if "text" in data:
                            return data["text"]
                        else:
                            error = f"No text field for resource {single_code}."
                    else:
                        error = f"Failed connection to OMIM API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                elif system == Ontologies.HGNC["url"]:
                    url = f"https://rest.ensembl.org/xrefs/id/{single_code}?external_db=HGNC;content-type=application/json;all_levels=1"
                    response = send_query_to_api(url=url, secret=None, access_type=AccessTypes.USER_AGENT)
                    if response is None:
                        error = f"Failed connection to HGNC API."
                    elif response.status_code == 200:
                        data = parse_json_response(response)
                        if len(data) > 0 and "description" in data[0]:
                            return data[0]["description"]
                        else:
                            error = f"No text field for resource {single_code}."
                    else:
                        error = f"Failed connection to HGNC API."
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=error)
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
                else:
                    quality_stats.add_failed_api_call(system=system, code=single_code, api_error=f"Unknown ontology {system}")
                    return DEFAULT_ONTOLOGY_RESOURCE_LABEL
            except Exception as e:
                # the API could not be queried, returning empty string
                quality_stats.add_failed_api_call(system=system, code=single_code, api_error=e.args[0])
                return DEFAULT_ONTOLOGY_RESOURCE_LABEL
        else:
            return DEFAULT_ONTOLOGY_RESOURCE_LABEL

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)

    @classmethod
    def from_json(cls, json_or: dict, quality_stats: QualityStatistics):  # returns an OntologyResource
        # fill a new OntologyResource instance with a JSON-encoded OntologyResource
        the_system = Ontologies.get_enum_from_url(json_or["system"]) if "system" in json_or else ""
        the_code = json_or["code"] if "code" in json_or else ""
        the_label = json_or["label"] if "label" in json_or else None
        the_or = OntologyResource(system=the_system, code=the_code, label=the_label, quality_stats=quality_stats)
        return the_or

    def __eq__(self, other):
        if not isinstance(other, OntologyResource):
            raise TypeError(f"Could not compare the current instance with an instance of type {type(other)}.")

        # we do not use the display  because this would lead to unequal instances
        # if provided descriptions differ from one hospital to another
        return self.system == other.system and self.code == other.code
