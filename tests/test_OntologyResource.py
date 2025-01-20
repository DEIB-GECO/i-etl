import unittest

from entities.OntologyResource import OntologyResource
from enums.Ontologies import Ontologies


class TestOntologyResource(unittest.TestCase):

    FULL_CODE_1 = "422549004|patient-related identification code|"
    FULL_CODE_2 = "264275001 | Fluorescence polarization immunoassay technique |  :  250895007| Intensity  change |   "
    FULL_CODE_3 = "  365471004 |   finding of  details of   relatives  |    :247591002|  affected |=   (410515003|known present( qualifier value) |= 782964007|  genetic disease |)"
    FULL_CODE_4 = "GO:0000380"

    def test_compute_concat_codes(self):
        o1 = OntologyResource(system=Ontologies.SNOMEDCT, code=TestOntologyResource.FULL_CODE_1, label=None, quality_stats=None)
        o2 = OntologyResource(system=Ontologies.SNOMEDCT, code=TestOntologyResource.FULL_CODE_2, label=None, quality_stats=None)
        o3 = OntologyResource(system=Ontologies.SNOMEDCT, code=TestOntologyResource.FULL_CODE_3, label=None, quality_stats=None)
        o4 = OntologyResource(system=Ontologies.GENE_ONTOLOGY, code=TestOntologyResource.FULL_CODE_4, label=None, quality_stats=None)

        assert o1.code == "422549004"
        assert o2.code == "264275001:250895007"
        assert o3.code == "365471004:247591002=(410515003=782964007)"
        assert o4.code == "0000380"

    def test_compute_concat_names(self):
        o1 = OntologyResource(system=Ontologies.SNOMEDCT, code=TestOntologyResource.FULL_CODE_1, label=None, quality_stats=None)
        o2 = OntologyResource(system=Ontologies.SNOMEDCT, code=TestOntologyResource.FULL_CODE_2, label=None, quality_stats=None)
        o3 = OntologyResource(system=Ontologies.SNOMEDCT, code=TestOntologyResource.FULL_CODE_3, label=None, quality_stats=None)
        o4 = OntologyResource(system=Ontologies.GENE_ONTOLOGY, code=TestOntologyResource.FULL_CODE_4, label=None, quality_stats=None)

        assert o1.label == "Patient-related Identification code"
        assert o2.label == "Fluorescence polarization immunoassay technique:Intensity change"
        assert o3.label == "Details of relatives:Affecting=(Known present=Genetic disease)"
        assert o4.label == "alternative mRNA splicing via spliceosome"  # removed comma between splicing and via because this is a snomed operator
