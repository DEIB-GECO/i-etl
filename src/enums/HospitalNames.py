import inflection

from enums.EnumAsClass import EnumAsClass
from utils.assertion_utils import is_not_nan
from utils.str_utils import process_spaces


class HospitalNames(EnumAsClass):
    # hospital names HAVE TO be normalized by hand here because we can't refer to static methods
    # because they do not exist yet in the execution context
    IT_BUZZI_UC1 = "it_buzzi_uc1"
    RS_IMGGE = "rs_imgge"
    ES_HSJD = "es_hsjd"
    IT_BUZZI_UC3 = "it_buzzi_uc3"
    ES_TERRASSA = "es_terrassa"
    DE_UKK = "de_ukk"
    ES_LAFE = "es_lafe"
    IL_HMC = "il_hmc"
    TEST_H1 = "test_h1"
    TEST_H2 = "test_h2"
    TEST_H3 = "test_h3"
    EXPES_EDA = "eda"
    EXPES_COVID = "covid"
    EXPES_KIDNEY = "kidney"

    @classmethod
    def short(cls, hospital_name) -> str:
        if "_" in hospital_name:
            return hospital_name.split("_")[1]  # the second part corresponds to the hospital name (only)
        else:
            # this is a simple hospital name, such as covid, eda, etc.
            # we can return it directly
            return hospital_name

    @classmethod
    def normalize(cls, hospital_name: str) -> str:
        if is_not_nan(hospital_name):
            hospital_name = process_spaces(hospital_name)
            hospital_name = hospital_name.replace(" ", "_")  # add missing _ in hospital name if needed
            return inflection.underscore(hospital_name).lower()
        else:
            return hospital_name
