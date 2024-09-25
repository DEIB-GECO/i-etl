import re
from urllib.parse import quote

from enums.AccessTypes import AccessTypes
from enums.Ontologies import Ontologies
from constants.defaults import DEFAULT_CODING_DISPLAY, SNOMED_OPERATORS_LIST, SNOMED_OPERATORS_STR
from statistics.QualityStatistics import QualityStatistics
from utils.api_utils import send_query_to_api, parse_json_response, parse_xml_response, parse_html_response
from utils.assertion_utils import is_not_nan
from utils.setup_logger import log
from utils.str_utils import process_spaces, remove_operators_in_strings, remove_specific_tokens

REPORTING_NOT_WORKING_QUERIES = []


class OntologyResource:
    def __init__(self, ontology: Ontologies, full_code: str, quality_stats: QualityStatistics | None):
        self.quality_stats = quality_stats if quality_stats is not None else QualityStatistics(record_stats=False)
        # full code corresponds to a (simple or complex) ontology code with or without names
        # e.g., 406506008
        # or 406506008|Attention deficit hyperactivity disorder|
        # or 726527001|Weight|:410671006|Date|
        # or 3332001|Occipitofrontal diameter of head|:246454002|Occurrence|=3950001|Birth|
        if is_not_nan(ontology) and is_not_nan(full_code):
            self.ontology = ontology
            self.full_code = full_code
            # log.info(self.full_code)
            self.full_code = Ontologies.remove_prefix(code=full_code)
            self.full_code = process_spaces(input_string=self.full_code)
            self.full_code = re.sub(r" *(["+SNOMED_OPERATORS_STR+"]+) *", r"\1", self.full_code)  # remove spaces around operators; r"\1" means: replace with first captured group
            self.full_code = remove_operators_in_strings(input_string=self.full_code)  # for every label inside |, '' or "", we remove possible operators
            # log.info(self.full_code)
            self.elements = []  # this contains the list of elements (codes and operators) in self.full_code
            self.concat_codes = ""  # this joins all elements in self.elements (codes and operators)
            self.concat_names = ""  # this joins the name of each code with operators (based on self.elements)
            self.compute_elements()
        else:
            # this happens when there is no ontology resource associated to a given column (or categorical value)
            # or when the ontology could not be found in the list of ontologies
            self.full_code = None
            self.ontology = None
            self.elements = None
            self.concat_codes = None
            self.concat_names = None

    def compute_elements(self) -> None:
        if self.full_code is None:
            pass
        else:
            regex_elements = re.split(r"(?=["+SNOMED_OPERATORS_STR+"])|(?<=["+SNOMED_OPERATORS_STR+"])", self.full_code)
            # now self.elements may still contain spaces
            # therefore, we remove them manually afterward
            for element in regex_elements:
                if element == "" or element is None or element == " ":
                    # the regex sometimes returns empty or None elements, we skip them
                    # it may also identify spaces around operators, we skip them too
                    pass
                else:
                    self.elements.append(element)
        # log.info(f"elements is: {self.elements}")

    def compute_concat_codes(self) -> None:
        for i in range(len(self.elements)):
            element = self.elements[i]
            if element not in SNOMED_OPERATORS_LIST:
                # this is not an operator
                if i-1 >= 0 and self.elements[i-1] == "|":
                    # we are in an annotation, thus we skip it
                    pass
                elif element.startswith("\""):
                    # this is a constant, thus we keep it and only process space (not caps)
                    self.concat_codes += process_spaces(input_string=element)
                else:
                    # this is a code
                    self.concat_codes += Ontologies.normalize_code(ontology_code=element)
            else:
                # this is an operator, we add it only if this in not the pipe
                if element != "|":
                    self.concat_codes += element
        # log.info(f"concat codes is: {self.concat_codes}")

    def compute_concat_names(self) -> None:
        self.concat_names = ""
        for i in range(len(self.elements)):
            element = self.elements[i]
            if element not in SNOMED_OPERATORS_LIST and not element.startswith("\""):  # " is used for surrounding constants, e.g., "HPO" in 278201002|Classification|="HPO"
                # this is not an operator, nor a constant
                if i - 1 >= 0 and self.elements[i - 1] == "|":
                    # we are in an annotation, thus we skip it
                    pass
                else:
                    # this is a code, we get its label (name)
                    resource_label = self.get_resource_label_from_api(single_ontology_code=element)
                    if resource_label is None:
                        pass
                    else:
                        resource_label = process_spaces(input_string=resource_label)
                        resource_label = remove_specific_tokens(input_string=resource_label, tokens=["(property)", "- finding", "-finding", "(qualifier value)", "(observable entity)", "(social concept)", "(procedure)", "(assessment scale)", "- action", "-action", "- attribute", "-attribute"])  # useless and may break parsing (due to parenthesis and dash)
                        resource_label = remove_specific_tokens(input_string=resource_label, tokens=SNOMED_OPERATORS_LIST)  # if we don't remove them, this will break future parsing
                        resource_label = process_spaces(input_string=resource_label)
                        self.concat_names += resource_label
            else:
                # this is an operator, we add it only if this in not the pipe
                if element != "|":
                    self.concat_names += element
        # log.info(f"concat names is: {self.concat_names}")

    def get_resource_label_from_api(self, single_ontology_code: str) -> str:
        # column_name is to be used when the display of the Coding could not be computed with any of the APIs
        compute_from_api = True
        if compute_from_api:
            try:
                if self.ontology == Ontologies.SNOMEDCT:
                    url_resource = quote(f"http://purl.bioontology.org/ontology/SNOMEDCT/{single_ontology_code}", safe="")
                    url = f"http://data.bioontology.org/ontologies/SNOMEDCT/classes/{url_resource}"
                    response = send_query_to_api(url=url, secret="d6fb9c05-3309-4158-892f-65434a9133b9",
                                                 access_type=AccessTypes.API_KEY_IN_URL)
                    data = parse_json_response(response)
                    try:
                        return data["prefLabel"]
                    except Exception as e:
                        self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                        return DEFAULT_CODING_DISPLAY
                elif self.ontology == Ontologies.LOINC:
                    url = f"https://loinc.regenstrief.org/searchapi/loincs?query={single_ontology_code}"
                    response = send_query_to_api(url=url, secret="nbarret d7=47@xiz$g=-Ns",
                                                 access_type=AccessTypes.AUTHENTICATION)
                    data = parse_json_response(response)
                    try:
                        return data["Results"][0]["COMPONENT"]
                    except Exception as e:
                        self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                        return DEFAULT_CODING_DISPLAY
                elif self.ontology == Ontologies.PUBCHEM:
                    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{single_ontology_code}/description/JSON"
                    response = send_query_to_api(url, secret=None, access_type=AccessTypes.USER_AGENT)
                    data = parse_json_response(response)
                    try:
                        return data["InformationList"]["Information"][0]["Title"]
                    except Exception as e:
                        self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                        return DEFAULT_CODING_DISPLAY
                elif self.ontology == Ontologies.CLIR:
                    # TODO Nelly: code this
                    self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error="No API access for the CLIR ontology.")
                    return DEFAULT_CODING_DISPLAY
                elif self.ontology == Ontologies.GSSO:
                    iri = f"http://purl.obolibrary.org/obo/{single_ontology_code.upper()}"  # we need to upper case the GSSO_, otherwise the API returns None
                    url = f"https://ontobee.org/ontology/GSSO?iri={iri}"
                    response = send_query_to_api(url=url, secret="d6fb9c05-3309-4158-892f-65434a9133b9",
                                                 access_type=AccessTypes.API_KEY_IN_BEARER)
                    data = parse_xml_response(response)  # data is an XML document
                    classes = data.getElementsByTagName('Class')
                    try:
                        for one_class in classes:
                            if one_class.getAttribute("rdf:about") == iri:
                                if len(one_class.getElementsByTagName("rdfs:label")) > 0:
                                    return one_class.getElementsByTagName("rdfs:label")[0].childNodes[0].data
                    except Exception as e:
                        self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                        return DEFAULT_CODING_DISPLAY
                elif self.ontology == Ontologies.ORPHANET:
                    url = f"https://api.orphacode.org/EN/ClinicalEntity/orphacode/{single_ontology_code}/Name"
                    response = send_query_to_api(url=url, secret="nbarret", access_type=AccessTypes.API_KEY_IN_HEADER)
                    data = parse_json_response(response)
                    try:
                        return data["Preferred term"]
                    except Exception as e:
                        self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                        return DEFAULT_CODING_DISPLAY
                elif self.ontology == Ontologies.GENE_ONTOLOGY:
                    # as of 03/09/2024, this ontology is queried by accessnig the sebpage describing the resource
                    # it seems that there is an RDF query tool, but it is not sure that this can be queried as an API
                    # and there is no documentation on existing properties to query some codes
                    url = f"https://amigo.geneontology.org/amigo/term/GO:{single_ontology_code}"
                    response = send_query_to_api(url=url, secret=None, access_type=AccessTypes.USER_AGENT)
                    data = parse_html_response(response)
                    try:
                        return data.select_one("div.page-header > h1").text
                    except Exception as e:
                        self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                        return DEFAULT_CODING_DISPLAY
                else:
                    self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=f"Unknown ontology {self.ontology}")
                    return DEFAULT_CODING_DISPLAY
            except Exception as e:
                # the API could not be queried, returning empty string
                self.quality_stats.add_failed_api_call(ontology_name=self.ontology["name"], id_code=single_ontology_code, api_error=e.args[0])
                return DEFAULT_CODING_DISPLAY
        else:
            return DEFAULT_CODING_DISPLAY

    def to_json(self) -> dict:
        return {
            "ontology": self.ontology["url"],
            "code": self.full_code,
            "concat_codes": self.concat_codes,
            "concat_names": self.concat_names
        }
