import unittest

from datatypes.OntologyResource import OntologyResource
from enums.Ontologies import Ontologies


class TestOntologyResource(unittest.TestCase):

    FULL_CODE_1 = "422549004|patient-related identification code|"
    FULL_CODE_2 = "264275001 | Fluorescence polarization immunoassay technique |  :  250895007| Intensity  change |   "
    FULL_CODE_3 = "  365471004 |   finding of  details of   relatives  |    :247591002|  affected |=   (410515003|known present( qualifier value) |= 782964007|  genetic disease |)"
    FULL_CODE_4 = "GO:0000380"

    def test_constructor(self):
        o1 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_1, quality_stats=None)
        o2 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_2, quality_stats=None)
        o3 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_3, quality_stats=None)
        o4 = OntologyResource(ontology=Ontologies.GENE_ONTOLOGY, full_code=TestOntologyResource.FULL_CODE_4, quality_stats=None)

        assert o1.full_code == "422549004|patient-related identification code|"
        assert o1.elements == ['422549004', '|', 'patient-related identification code', '|']
        assert o2.full_code == "264275001|Fluorescence polarization immunoassay technique|:250895007|Intensity change|"
        assert o2.elements == ['264275001', '|', 'Fluorescence polarization immunoassay technique', '|', ':', '250895007', '|', 'Intensity change', '|']
        assert o3.full_code == "365471004|finding of details of relatives|:247591002|affected|=(410515003|known present qualifier value |=782964007|genetic disease|)"
        assert o3.elements == ['365471004', '|', 'finding of details of relatives', '|', ':', '247591002', '|', 'affected', '|', '=', '(', '410515003', '|', 'known present qualifier value ', '|', '=', '782964007', '|', 'genetic disease', '|', ')']
        assert o4.full_code == "0000380"  # remove prefix because it contains :, which is a snomed operator
        assert o4.elements == ["0000380"]  # same as above

    def test_compute_concat_codes(self):
        o1 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_1, quality_stats=None)
        o1.compute_concat_codes()
        o2 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_2, quality_stats=None)
        o2.compute_concat_codes()
        o3 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_3, quality_stats=None)
        o3.compute_concat_codes()
        o4 = OntologyResource(ontology=Ontologies.GENE_ONTOLOGY, full_code=TestOntologyResource.FULL_CODE_4, quality_stats=None)
        o4.compute_concat_codes()

        assert o1.concat_codes == "422549004"
        assert o2.concat_codes == "264275001:250895007"
        assert o3.concat_codes == "365471004:247591002=(410515003=782964007)"
        assert o4.concat_codes == "0000380"

    def test_compute_concat_names(self):
        o1 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_1, quality_stats=None)
        o1.compute_concat_names()
        o2 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_2, quality_stats=None)
        o2.compute_concat_names()
        o3 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code=TestOntologyResource.FULL_CODE_3, quality_stats=None)
        o3.compute_concat_names()
        o4 = OntologyResource(ontology=Ontologies.GENE_ONTOLOGY, full_code=TestOntologyResource.FULL_CODE_4, quality_stats=None)
        o4.compute_concat_names()

        assert o1.concat_names == "Patient-related Identification code"
        assert o2.concat_names == "Fluorescence polarization immunoassay technique:Intensity change"
        assert o3.concat_names == "Details of relatives:Affecting=(Known present=Genetic disease)"
        assert o4.concat_names == "alternative mRNA splicing via spliceosome" # removed comma between splicing and via because this is a snomed operator
