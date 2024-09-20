import sys
import headfake
import headfake.field
import pandas as pd
import scipy
import random

from CM_MODULES.Patient import Patient, BirthData

def generate_clinical_dataset():
    ID_STUDY = Patient.generate_patient_id("ID_STUDY", min_value=1, length=7, prefix="UC3_")
    REDCAP_ID = Patient.generate_patient_id("REDCAP_ID", min_value=1, length=7, prefix="REDCAP_")
    GENDER = headfake.field.OptionValueField(name="GENDER", probabilities={"M":0.4, "F":0.4, "Other":0.2})
    BIRTHDATE = BirthData.generate_date_of_birth(field_name="DOB", distribution=scipy.stats.uniform, min_year=5, max_year=50, date_format="%d/%m/%Y")
    #ID_STUDY TODO: Duplicated field?
    SIB_NUM = headfake.field.OptionValueField(name="SIB_NUM", probabilities={"0":0.2,"1":0.2,"2":0.2,"3":0.2,"4":0.2})
    SIB_R = None
    CHIL = None
    VISIT_DATE = None
    AGE = None
    WEIGHT = None
    HEIGHT = None
    BP_SYST = headfake.field.OptionValueField(name="BP_SYS", probabilities={"100":0.2,"110":0.2,"120":0.2,"130":0.2,"140":0.2})
    BP_DIAST = headfake.field.OptionValueField(name="BP_DIAST", probabilities={"50":0.2,"60":0.2,"70":0.2,"80":0.2,"90":0.2})
    HMP = headfake.field.OptionValueField(name="HMP", probabilities={"yes":0.5, "no":0.5})
    HPP = headfake.field.OptionValueField(name="HPP", probabilities={"yes":0.5, "no":0.5})
    AMDP_C = headfake.field.OptionValueField(name="AMDP_C", probabilities={"0":0.3,"1":0.3,"2":0.2,"3":0.2})
    AMDP_O = headfake.field.OptionValueField(name="AMDP_O", probabilities={"0":0.2,"1":0.2,"2":0.2,"3":0.2,"4":0.2})
    AMDP_A = headfake.field.OptionValueField(name="AMDP_A", probabilities={"0":0.2,"1":0.2,"2":0.2,"3":0.2,"4":0.1,"5":0.1})
    AMDP_FT = headfake.field.OptionValueField(name="AMDP_FT", probabilities={"0":0.1,"1":0.1,"2":0.1,"3":0.1,"4":0.1,"5":0.1,"6":0.1,"7":0.1,"8":0.1,"9":0.1})
    AMDP_WC = headfake.field.OptionValueField(name="AMDP_WC", probabilities={"0":0.1,"1":0.1,"2":0.1,"3":0.1,"4":0.2,"5":0.2,"6":0.2})
    AMDP_D = headfake.field.OptionValueField(name="AMDP_D", probabilities={"0":0.1,"1":0.1,"2":0.1,"3":0.1,"4":0.1,"5":0.1,"6":0.1,"7":0.1,"8":0.1,"9":0.1})
    AMDP_P = headfake.field.OptionValueField(name="AMDP_P", probabilities={"0":0.1,"1":0.1,"2":0.1,"3":0.1,"4":0.2,"5":0.2,"6":0.2})
    AMDP_E = headfake.field.OptionValueField(name="AMDP_E", probabilities={"0":0.2,"1":0.2,"2":0.2,"3":0.2,"4":0.1,"5":0.1})
    AMDP_AF = headfake.field.OptionValueField(name="AMDP_AF", probabilities={"0":0.1,"1":0.1,"2":0.1,"3":0.1,"4":0.1,"5":0.1,"6":0.1,"7":0.1,"8":0.1,"9":0.05,"10":0.02,"11":0.03})
    AMDP_D = headfake.field.OptionValueField(name="AMDP_D", probabilities={"0":0.2,"1":0.2,"2":0.1,"3":0.1,"4":0.1,"5":0.1,"6":0.1,"7":0.1})
    AMDP_SB = headfake.field.OptionValueField(name="AMDP_SB", probabilities={"0":0.3,"1":0.3,"2":0.2,"3":0.2})
    AMDP_AG = headfake.field.OptionValueField(name="AMDP_AG", probabilities={"0":0.3,"1":0.3,"2":0.2,"3":0.2})
    AMDP_SL = None
    RUM_THO = headfake.field.OptionValueField(name="RUM_THO", probabilities={"0":0.3,"1":0.3,"2":0.2,"3":0.2})
    EM_S = headfake.field.OptionValueField(name="EM_S", probabilities={"0":0.2,"1":0.2,"2":0.2,"3":0.2,"4":0.2})
    EM_F = headfake.field.OptionValueField(name="EM_F", probabilities={"yes":0.5, "no":0.5})
    EM_C = headfake.field.OptionValueField(name="EM_C", probabilities={"yes":0.5, "no":0.5})
    EM_R = headfake.field.OptionValueField(name="EM_R", probabilities={"yes":0.5, "no":0.5})
    CON_GS = headfake.field.OptionValueField(name="CON_GS", probabilities={"yes":0.5, "no":0.5})
    BE_MHS = headfake.field.OptionValueField(name="BE_MHS", probabilities={"yes":0.5, "no":0.5})
    PB_P = headfake.field.OptionValueField(name="PB_P", probabilities={"yes":0.5, "no":0.5})
    PB_V = headfake.field.OptionValueField(name="PB_V", probabilities={"yes":0.5, "no":0.5})
    PB_E = headfake.field.OptionValueField(name="PB_E", probabilities={"yes":0.5, "no":0.5})
    PB_VV = headfake.field.OptionValueField(name="PB_VV", probabilities={"yes":0.5, "no":0.5})
    PB_PV = headfake.field.OptionValueField(name="PB_PV", probabilities={"yes":0.5, "no":0.5})
    CB_P = headfake.field.OptionValueField(name="CB_P", probabilities={"yes":0.5, "no":0.5})
    CB_V = headfake.field.OptionValueField(name="CB_V", probabilities={"yes":0.5, "no":0.5})
    CB_E = headfake.field.OptionValueField(name="CB_E", probabilities={"yes":0.5, "no":0.5})
    CB_VV = headfake.field.OptionValueField(name="CB_VV", probabilities={"yes":0.5, "no":0.5})
    CB_PV = headfake.field.OptionValueField(name="CB_PV", probabilities={"yes":0.5, "no":0.5})
    SH_AGE = None
    SH_P = headfake.field.OptionValueField(name="SH_P", probabilities={"0":0.2,"1":0.2,"2":0.2,"3":0.2,"4":0.1,"5":0.1})
    SH_MI = headfake.field.OptionValueField(name="SH_MI", probabilities={"yes":0.5, "no":0.5})
    SH_R = headfake.field.OptionValueField(name="SH_R", probabilities={"yes":0.5, "no":0.5})
    SL = headfake.field.OptionValueField(name="SL", probabilities={"0":0.3,"1":0.3,"2":0.2,"3":0.2})
    SCQ_T = None
    CBCL_D_IP = None
    CBCL_P_IP = None
    CBCL_D_EP = None
    CBCL_D_EP = None
    CBCL_D_T = None
    CBCL_P_T = None
    YSR_D_IP = None
    YSR_P_IP = None
    YSR_D_EP = None
    YSR_P_EP = None
    YSR_D_T = None
    YSR_P_T = None
    BDI_D = None
    BDI_P = None
    CPRS_D_ADHD = None
    CPRS_P_ADHD = None
    CTRS_D_ADHD = None
    CTRS_P_ADHD = None
    TAS_26 = None
    SBQ_R = None
    SBQ_ASC = None
    HADS_D_A = None
    HADS_D_D = None
    RRS_22 = None
    OCI_D = None
    OCI_P = None
    OCI_CP = None
    GAF = None
    ADIS = None
    BFCRS_SCS = None
    BFCRS_SS = None
    RCADS_25 = None
    WURS_D = None
    WURS_P = None
    WURS_CP = None
    DAST = None
    ASA_A = None
    ADAT_A = None
    WGS_ID = None
    
    fs = headfake.Fieldset(fields=[ID_STUDY,REDCAP_ID,GENDER,BIRTHDATE,SIB_NUM,SIB_R,CHIL,VISIT_DATE,AGE,WEIGHT,HEIGHT,
                                    BP_SYST,BP_DIAST,HMP,HPP,AMDP_C,AMDP_O,AMDP_A,AMDP_FT,AMDP_WC,AMDP_D,AMDP_P,AMDP_E,
                                    AMDP_AF,AMDP_D,AMDP_SB,AMDP_AG,AMDP_SL,RUM_THO,EM_S,EM_F,EM_C,EM_R,CON_GS,BE_MHS,
                                    PB_P,PB_V,PB_E,PB_VV,PB_PV,CB_P,CB_V,CB_E,CB_VV,CB_PV,SH_AGE,SH_P,SH_MI,SH_R,SL,SCQ_T,
                                    CBCL_D_IP,CBCL_P_IP,CBCL_D_EP,CBCL_D_EP,CBCL_D_T,CBCL_P_T,YSR_D_IP,YSR_P_IP,YSR_D_EP,
                                    YSR_P_EP,YSR_D_T,YSR_P_T,BDI_D,BDI_P,CPRS_D_ADHD,CPRS_P_ADHD,CTRS_D_ADHD,CTRS_P_ADHD,
                                    TAS_26,SBQ_R,SBQ_ASC,HADS_D_A,HADS_D_D,RRS_22,OCI_D,OCI_P,OCI_CP,GAF,ADIS,BFCRS_SCS,
                                    BFCRS_SS,RCADS_25,WURS_D,WURS_P,WURS_CP,DAST,ASA_A,ADAT_A,WGS_ID])
    
    return fs

def generate_medication_dataset(clinical_df):

    ID_STUDY = clinical_df["ID_STUDY"]
    DRUG = []
    DRUG_D = []
    CHA_RU = []
    
    for id in ID_STUDY:
        DRUG.append(random.choice(["1","2","3","4"]))
        DRUG_D.append(None)
        CHA_RU.append(random.choice(["new","add","change"]))

    data = {"id_study":ID_STUDY, "drug": DRUG, "drug_d": DRUG_D, "cha_rut": CHA_RU}
    
    df = pd.DataFrame(data=data)
    
    return df

def main():
    if len(sys.argv) < 3:
        print("Ussage: py BUZZI.py <num_rows> <output_file_name_1> <output_file_name_2>")
    else:
        num_rows = int(sys.argv[1])
        output_file_1 = sys.argv[2]
        output_file_2 = sys.argv[3]
        
        # Create the baseline clinical table
        fieldset = generate_clinical_dataset()
        hf = headfake.HeadFake.from_python({"fieldset":fieldset})
        data_1 = hf.generate(num_rows=num_rows)

        # Create CSV output
        data_1.to_csv(output_file_1, index=False)
        print(f"Clinical results written to {output_file_1}")
        
        # Create the genetic table
        data_2 = generate_medication_dataset(data_1)
        
        # Create the CSV output
        data_2.to_csv(output_file_2, index=False)
        print(f"Medication results written to {output_file_2}")
        

if __name__ == "__main__":
    main()