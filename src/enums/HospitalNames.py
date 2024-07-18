from enum import Enum

from utils.utils import normalize_hospital_name


class HospitalNames(Enum):
    IT_BUZZI_UC1 = normalize_hospital_name("IT_BUZZI_UC1")
    RS_IMGGE = normalize_hospital_name("RS_IMGGE")
    ES_HSJD = normalize_hospital_name("ES_HSJD")
    IT_BUZZI_UC3 = normalize_hospital_name("IT_BUZZI_UC3")
    ES_TERRASSA = normalize_hospital_name("ES_TERRASSA")
    DE_UKK = normalize_hospital_name("DE_UKK")
    ES_LAFE = normalize_hospital_name("ES_LAFE")
    IL_HMC = normalize_hospital_name("IL_HMC")
    TEST_H1 = normalize_hospital_name("TEST_H1")
    TEST_H2 = normalize_hospital_name("TEST_H2")
    TEST_H3 = normalize_hospital_name("TEST_H3")

    @classmethod
    def get(cls) -> list[str]:
        accepted_hospital_names = []
        for hospital_name in HospitalNames:
            if not hospital_name.value.startswith("test"):
                accepted_hospital_names.append(hospital_name.value)
        return accepted_hospital_names

    @classmethod
    def get_as_iterator(cls) -> iter:
        return iter(HospitalNames.get())
