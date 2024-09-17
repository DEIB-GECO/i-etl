from enums.EnumAsClass import EnumAsClass


class DiagnosisColumns(EnumAsClass):
    # ontology names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    FREE_TEXT = "diagnosis"
    CLASSIFICATION = "classification"
    STANDARD_NAME = "diagnosis_name"
    ORPHANET_CODE = "orpha_net"
