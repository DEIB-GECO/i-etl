import sys
import headfake
import headfake.field
import headfake.transformer
import scipy

from CM_MODULES.Patient import Patient, BirthData
from CM_MODULES.Variant import Variant, VariantInheritance
from CM_MODULES.Diagnosis import Diagnosis
from CM_MODULES.MedicalHistory import MedicalHistory, IDTest

ZIGOSITY2VAR = {"Heterozygous":0.5, "Homozygous":0.4, "Hemizygous":0.1}
INHERITANCE = {"Dominant":0.5,"Recessive":0.5}
CONSANGUINITY = {"No":0.8, "Yes":0.1, "No information":0.1}
AFFECTED_RELATIVES = {"No":0.6, "Yes":0.3, "No information":0.1}
WALK_INDEPENDENTLY = {"No":0.3, "Yes":0.6, "No information":0.1}
SPEAK_INDEPENDENTLY = {"No":0.3, "Yes":0.6, "No information":0.1}
DEVELOPMENTAL_DISORDER = {"No":0.3, "Yes":0.6, "No information":0.1}
INTELECTUAL_DISABILITY = {"No":0.3, "Yes":0.6, "No information":0.1}
AUTISM_SPECTRUM_DISORDER = {"No":0.3, "Yes":0.6, "No information":0.1}
ATENTION_DEFICIT_HIPERACTIVITY = {"No":0.3, "Yes":0.6, "No information":0.1}
LANGUAGE_ABSENCE = {"No":0.3, "Yes":0.6, "No information":0.1}
PHOTO_AVAILABLE = {"No":0.3, "Yes":0.6, "No information":0.1}

def generate_baseline_clinical_dataset():
    patient_id = Patient.generate_patient_id(field_name="Patient ID", min_value=1, length=8, prefix="SJD_")
    zigosity2var = Variant.generate_zygosity(field_name="Zigosity2var", options=ZIGOSITY2VAR)
    inheritance = VariantInheritance.generate_inheritance(field_name="Inheritance", options=INHERITANCE)
    dx = Diagnosis.generate_diagnosis_name(field_name="Dx", disease_file_name="DATA/SJD_Diseases.csv", key_field="Disease")
    orpha_id = headfake.field.LookupMapFileField(lookup_value_field="Orphanet", map_file_field="Dx", hidden=True)
    hpo = Diagnosis.get_symptoms(field_name="HPO", omim_id=None, orpha_id=orpha_id, symptoms_max_number=5)
    dob = BirthData.generate_date_of_birth(field_name="DOB", distribution=scipy.stats.uniform, min_year=0, max_year=0, date_format="%d/%m/%Y")
    sex = Patient.generate_sex(field_name="Sex", male_value="male", female_value="female")
    consanguinity = MedicalHistory.generate_consanguinity(field_name="Consanguinity", options=CONSANGUINITY)
    relatives = MedicalHistory.generate_affected_relatives(field_name="Relatives", options=AFFECTED_RELATIVES)
    walk_indep = MedicalHistory.generate_walk_independently(field_name="Walkindep", options=WALK_INDEPENDENTLY)
    walk = headfake.field.LookupField(field="Walkindep", hidden=True)
    walk_age = MedicalHistory.generate_walk_age("Walkage", walk_indep=walk)
    speak_indep = MedicalHistory.generate_speak_independently(field_name="Speak", options=SPEAK_INDEPENDENTLY)
    speak = headfake.field.LookupField(field="Speak", hidden=True)
    speak_age = MedicalHistory.generate_speak_age(field_name="Speakage", speak_indep=speak)
    dd = MedicalHistory.generate_developmental_disorder(field_name="DD", options=DEVELOPMENTAL_DISORDER)
    id = MedicalHistory.generate_speak_independently(field_name="ID", options=INTELECTUAL_DISABILITY)
    has_id = headfake.field.LookupField(field="ID", hidden=True)
    id_level = IDTest.generate_ID_test_to_string(field_name="IDlevel", hasID=has_id)
    asd = MedicalHistory.generate_speak_independently(field_name="ASD", options=AUTISM_SPECTRUM_DISORDER)
    adhd = MedicalHistory.generate_speak_independently(field_name="ADHD", options=ATENTION_DEFICIT_HIPERACTIVITY)
    language_abs = MedicalHistory.generate_language_absence(field_name="languageabs", options=LANGUAGE_ABSENCE)
    ga = headfake.field.OperationField(operator=None, first_value=27, second_value=40, operator_fn=BirthData.generate_gestational_age, hidden=True)
    gestational_age = BirthData.gestational_age_to_string(field_name="Gestationalage", ga=ga)
    weight_birth = BirthData.generate_weight(field_name="Weightbirth", gestational_age=gestational_age)
    length_birth = BirthData.generate_length(field_name="Lengthbirth", gestational_age=gestational_age)
    ofc_birth = BirthData.generate_ofc(field_name="OFCbirth", gestational_age=gestational_age)
    face = Diagnosis.get_facial_symptoms(field_name="Face", orpha_id=orpha_id, symptoms_max_number=5)
    face_hpo = Diagnosis.get_facial_HPO(field_name="Face HPO", symptoms_list=face)
    photo = MedicalHistory.generate_photo_available(field_name="PhotoAvailable", options=PHOTO_AVAILABLE)

    fs = headfake.Fieldset(fields=[patient_id, zigosity2var, inheritance, dx, orpha_id, hpo, dob, sex, 
                                   consanguinity, relatives, walk_indep, walk_age,
                                   speak_indep, speak_age, dd, id, id_level,
                                   asd, adhd, language_abs, gestational_age, weight_birth, length_birth, ofc_birth, 
                                   face, face_hpo, photo])
    
    return fs

def generate_biological_dataset():
    #Patient ID
    patient_id = None
    #WB
    wb = None
    #qPCR
    qpcr = None
    #Fluo
    fluo = None
    #Protinter
    protinter = None
    #Subcel
    subcel = None
    #Morcel
    morcel = None
    #Pathways
    pathways = None
    #Splicing
    splicing = None
    #CADD
    cadd = None
    #Metadome
    metadome = None
    #Missense3D
    missense_3d = None
    #Dynamut2
    dynamut = None
    #ACMG
    acmg = None
    #Alphamiss
    alphamiss = None
    

def generate_dynamic_clinical_dataset():
    # Patient ID
    patient_id = None
    # Weightdate1
    weight_date = None
    # Weight1
    weigth = None
    #Heightdate1
    height_date = None
    #Height1
    height = None
    #OFCdate1
    ofc_date = None
    #OFC1
    ofc = None


def generate_genomic_dataset():
    #Patient ID
    patient_id = None
    #GenomicRefSeq
    ref_seq = None
    #Startnt
    start = None
    #Endnt
    end = None
    #Ref
    ref = None
    #Alt
    alt = None
    #NM_transcript
    transcript = None
    #Variant
    variant = None
    #Type
    variant_type = None
    #Zygosity
    zygosity = None
    #ProteinRefSeq
    protein_name = None
    #Gene
    gene = None
    #ChromosomeNumber
    chromosome = None


def main():
    if len(sys.argv) < 5:
        print("Ussage: py SJD.py <num_rows> <output_file_name_1> <output_file_name_2> <output_file_name_3> <output_file_name_4>")
    else:
        num_rows = int(sys.argv[1])
        output_file_1 = sys.argv[2]
        output_file_2 = sys.argv[3]
        output_file_3 = sys.argv[4]
        output_file_4 = sys.argv[5]
        
        # Create the baseline clinical dataset
        fieldset = generate_baseline_clinical_dataset()
        hf = headfake.HeadFake.from_python({"fieldset":fieldset})
        data_1 = hf.generate(num_rows=num_rows)
        data_1.to_csv(output_file_1, index=False)
    
        print(f"Screening results written to {output_file_1}")
        
        # Create the biological dataset
        #data_2 = generate_biological_dataset()
        #data_2.to_csv(output_file_2, index=False)
        
        #print(f"Diagnosis results written to {output_file_2}")
        
        # Create the dynamic clinical dataset
        #data_3 = generate_dynamic_clinical_dataset()
        #data_3.to_csv(output_file_2, index=False)
        
        #print(f"Diagnosis results written to {output_file_3}")
        
        # Create the genomic dataset
        #data_4 = generate_genomic_dataset()
        #data_4.to_csv(output_file_2, index=False)
        
        #print(f"Diagnosis results written to {output_file_4}")
        
        

if __name__ == "__main__":
    main()