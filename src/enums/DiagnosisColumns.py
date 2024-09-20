from enums.EnumAsClass import EnumAsClass


class DiagnosisColumns(EnumAsClass):
    # ontology names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    NAME = "diagnosis_name"
    ACRONYM = "acronym"
    ORPHANET_CODE = "orpha_net"
    AFFECTED = "affected"
