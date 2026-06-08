from enums.EnumAsClass import EnumAsClass

class APIWait(EnumAsClass):
    WAIT_NLM_GENE = 0.5
    # With the NCBI API key the eutils limit is 10 req/s; each ClinVar lookup does
    # two sequential calls (esearch + esummary), so 0.25s stays comfortably under it.
    WAIT_NLM_CLINVAR = 0.25