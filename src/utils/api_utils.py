import base64
import xml.dom.minidom

from bs4 import BeautifulSoup
from requests import Response

from constants.defaults import API_SESSION
from enums.AccessTypes import AccessTypes
from utils.setup_logger import log


def send_query(url: str, headers: dict | None) -> Response | None:
    try:
        if headers is None:
            return API_SESSION.get(url, verify=True)
        else:
            return API_SESSION.get(url, headers=headers, verify=True)
    except Exception:
        # if there is an SSL problem, trying the same query without the SSL certificate verification
        try:
            if headers is None:
                return API_SESSION.get(url, verify=False)
            else:
                return API_SESSION.get(url, headers=headers, verify=False)
        except Exception as e:
            log.info(e)
            return None


# API ACCESS
def send_query_to_api(url, secret: str | None, access_type: AccessTypes) -> Response | None:
    # secret may contain an api key or (joint) username and password
    headers = {
        "User-Agent": "python-requests/2.31.0",
        "Accept-Encoding": "gzip, deflate",
        "Accept": "*/*",
        "Connection": "keep-alive",
    }
    if access_type == AccessTypes.USER_AGENT:
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.AUTHENTICATION:
        username = secret.split(" ")[0]
        password = secret.split(" ")[1]
        base64string = base64.b64encode(bytes('%s:%s' % (username, password), "ascii"))
        headers["Authorization"] = f"Basic {base64string.decode("utf-8")}"  # Make sure to prepend 'Bearer ' before your API key
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.API_KEY_IN_HEADER:
        headers["apiKey"] = secret
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.API_KEY_IN_BEARER:
        headers["Authorization"] = f"Bearer {secret}"
        return send_query(url=url, headers=headers)

    elif access_type == AccessTypes.API_KEY_IN_URL:
        url_with_apikey = f"{url}?apikey={secret}"
        return send_query(url=url_with_apikey, headers=None)
    else:
        # unknown access type
        return None


def parse_json_response(response):
    # we need to load x2 and to dump to have a "real" JSON dict, parseable by Python
    # otherwise, we have a JSON-like string or JSON-like text data
    if response is not None:
        return response.json()  # json.loads(json.dumps(json.loads(response.content)))
    else:
        return {}


def parse_xml_response(response):
    return xml.dom.minidom.parseString(response.content)


def parse_html_response(response):
    return BeautifulSoup(response.text, "html.parser")


def load_xml_file(filepath: str):
    return xml.dom.minidom.parse(filepath)
