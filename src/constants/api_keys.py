# Central store for the external API keys / credentials used by the ETL.
# Kept in one place so they are easy to find and rotate.
# NOTE: these are committed on purpose (private repo); do not share publicly.

# BioPortal (http://data.bioontology.org) - used for SNOMEDCT and GSSO lookups.
BIOPORTAL_API_KEY = "d6fb9c05-3309-4158-892f-65434a9133b9"

# LOINC (https://loinc.regenstrief.org) - HTTP basic auth, "<username> <password>".
LOINC_CREDENTIALS = "nbarret d7=47@xiz$g=-Ns"

# Orphanet (api.orphacode.org / api.orphadata.com) - passed as an apiKey header.
ORPHANET_API_KEY = "nbarret"

# OMIM (https://api.omim.org).
OMIM_API_KEY = "nfNEOscLNWWXdSmUoMLPPA"

# NCBI E-utilities API key. Passing it on eutils requests raises the rate limit
# from 3 to 10 requests/second. Generated at https://account.ncbi.nlm.nih.gov/
NCBI_API_KEY = "fef50183ebe7b58c47d6e8241f3d9ad2e708"
