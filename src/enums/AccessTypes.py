from enums.EnumAsClass import EnumAsClass


class AccessTypes(EnumAsClass):
    USER_AGENT = "user_agent"
    AUTHENTICATION = "authentication"
    API_KEY_IN_HEADER = "api_key_in_header"
    API_KEY_IN_BEARER = "api_key_in_bearer"
    API_KEY_IN_URL = "api_key_in_url"
