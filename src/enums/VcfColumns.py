from enums.EnumAsClass import EnumAsClass
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns

# cannot use HospitalNames.X in the enum, so I converted it to a dict in
# order to access the vcf columns based on the hospital names
vcf_columns = {
    HospitalNames.RS_IMGGE: MetadataColumns.normalize_name("path_vcf"),
    HospitalNames.ES_HSJD: MetadataColumns.normalize_name("VCF_Path"),
    HospitalNames.IT_BUZZI_UC1: None,
    HospitalNames.IT_BUZZI_UC3: None,
    HospitalNames.ES_TERRASSA: None,
    HospitalNames.DE_UKK: None,
    HospitalNames.ES_LAFE: None,
    HospitalNames.IL_HMC: None,
    HospitalNames.EXPES_EDA: None,
    HospitalNames.EXPES_COVID: None,
    HospitalNames.EXPES_KIDNEY: None,
    HospitalNames.TEST_H1: None,
    HospitalNames.TEST_H2: None,
    HospitalNames.TEST_H3: None
}
