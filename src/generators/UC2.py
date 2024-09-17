import sys
import headfake
import headfake.field
import headfake.transformer
import random
import sys
import datetime
import pandas as pd

from CM_MODULES.Patient import Patient
from CM_MODULES.Diagnosis import Diagnosis

uc2_diseases_df = pd.read_csv('DATA/UC2_Diseases.csv', sep=',')
variants_df = pd.read_csv('DATA/variants.csv', sep=",")

ETHNICITIES = ["ASH","NA","O","Y","Et","S","B","BU","I","IS","M","MR","G","Gr","T","Eg","U","AM","B","AC"]

def generate_baseline_clinical_dataset():
    patient_id = Patient.generate_patient_id("Patient ID", min_value=1, length=7, prefix="UC2_")
    yob = headfake.field.OperationField(name="YOB", operator=None, first_value=datetime.datetime.now().year-90, second_value=datetime.datetime.now().year, operator_fn=random.randint)
    sex = Patient.generate_sex(field_name="Sex", male_value="M", female_value="F")
    def select_ethnicity(ethnicities, *arguments):
        return random.choice(ethnicities)
    paternal_origin = headfake.field.OperationField(name="Porigin", operator=None, first_value=ETHNICITIES, second_value=None, operator_fn=select_ethnicity)
    maternal_origin = headfake.field.OperationField(name="Morigin", operator=None, first_value=ETHNICITIES, second_value=None, operator_fn=select_ethnicity)
    diagnosis = Diagnosis.generate_diagnosis_name(field_name="Dx", disease_file_name="DATA/UC2_Diseases.csv", key_field="DiseaseName")
    
    year_of_birth = headfake.field.LookupField(field="YOB", hidden=True)
    def get_onset(yob, *arguments):
        return int((datetime.datetime.now().year-yob)/2)
    
    onset = headfake.field.OperationField(name="Onset", operator=None, first_value=year_of_birth, second_value=None, operator_fn=get_onset)
    hearing_disability = headfake.field.OptionValueField(name="HR", probabilities={"no":0.5, "yes":0.5})
    
    def generate_comorbidities(*arguments):
        comorbidities = ["Hypertension","Diabetes Mellitus","Obesity","Hyperlipidemia", 
                        "Chronic Obstructive Pulmonary Disease","Coronary Artery Disease",
                        "Chronic Kidney Disease","Arthritis","Asthma"]
        result = []
        for comorb in comorbidities:
            has_comorb = random.choice(["yes","no"])
            result.append(f'{comorb} {has_comorb}')
        
        return (",").join(result)
    
    comorbidities = headfake.field.OperationField(name="Comorb", operator=None, first_value=None, second_value=None, operator_fn=generate_comorbidities)
    
    fs = headfake.Fieldset(fields=[patient_id, yob, sex, paternal_origin, maternal_origin, diagnosis, onset, hearing_disability, comorbidities])
    
    return fs


def generate_genetic_dataset(baseline_df):
    patient_id = baseline_df["Patient ID"]
    inheritance = []
    gene = []
    mutation1 = []
    mutation2 = []

    for id in patient_id:
        inheritance.append(random.choice(["AR","AD","XL"]))
        
        def get_gene(disease):
            x1 = uc2_diseases_df.loc[uc2_diseases_df['DiseaseName'] == disease]
            genes = None
            for i, row in x1.iterrows():
                genes = row["AffectedGenes"]
                break
            gene_list = genes.split(',')
        
            return random.choice(gene_list)
        
        disease = baseline_df.loc[baseline_df['Patient ID'] == id]['Dx'].values[0]
        gene.append(get_gene(disease))
        
        index = random.randint(0, len(variants_df)-1)
        conding_name = variants_df["coding_name"][index]
        protein_name = variants_df["protein_name"][index]
        if isinstance(protein_name, float):
            mutation1.append(f'{conding_name}')
        else:
            mutation1.append(f'{conding_name};{protein_name.replace("p.","p(")})')
        
    
        index = random.randint(0, len(variants_df)-1)
        conding_name = variants_df["coding_name"][index]
        protein_name = variants_df["protein_name"][index]
        if isinstance(protein_name, float):
            mutation2.append(f'{conding_name}')
        else:
            mutation2.append(f'{conding_name};{protein_name.replace("p.","p(")})')
        
    data = {'Patient ID': patient_id, 
            "Inheritance pattern": inheritance,
            "Gene": gene,
            "Mutation 1": mutation1,
            "Mutation 2": mutation2}
    
    df = pd.DataFrame(data=data)
    
    return df
        

def generate_dynamic_dataset(baseline_df, num_evolution_records):
    id_list = []
    age = []
    reva = []
    leva = []
    reref = []
    leref = []
    rodre = []
    rodle = []
    mixrea = []
    mixlea = []
    mixreb = []
    mixleb = []
    conereamp = []
    coneleamp = []
    conereimp = []
    coneleimp = []
    pvfre = []
    pvfle = []
    cvfre = []
    cvfle = []
    rcfr_iii = []
    rcfl_iii = []
    rcfr_v = []
    rcfl_v = []
    
    for id in baseline_df["Patient ID"]:
        age_onset = baseline_df.loc[baseline_df['Patient ID'] == id]['Onset'].values[0]
        for i in range(num_evolution_records):
            id_list.append(id)
            
            age_onset = age_onset + 1
            age.append(age_onset)
            
            visual_acuity = random.randint(a=0, b=100)
            reva.append(visual_acuity/100)
            
            visual_acuity = random.randint(a=0, b=100)
            leva.append(visual_acuity/100)
            
            myop_hiper =random.choice(["-", "+"])
            diop = random.randint(1, 8)
            med = random.choice([1,0])
            if med:
                reref.append(f'{myop_hiper}{diop}.{5}')
            else:
                reref.append(f'{myop_hiper}{diop}')
                             
            myop_hiper =random.choice(["-", "+"])
            diop = random.randint(1, 8)
            med = random.choice([1,0])
            
            if med:
                leref.append(f'{myop_hiper}{diop}.{5}')
            else:
                leref.append(f'{myop_hiper}{diop}')
            
            rodre.append(random.choice([0, "NA", random.randint(a=100, b=300)]))
            rodle.append(random.choice([0, "NA", random.randint(a=100, b=300)]))
            
            mixrea.append(random.choice([0, "NA", random.randint(a=150, b=250)]))
            mixlea.append(random.choice([0, "NA", random.randint(a=150, b=250)]))
            
            mixreb.append(random.choice([0, "NA", random.randint(a=300, b=600)]))
            mixleb.append(random.choice([0, "NA", random.randint(a=300, b=600)]))
            
            conereamp.append(random.choice([0, "NA", random.randint(a=20, b=60)]))
            coneleamp.append(random.choice([0, "NA", random.randint(a=20, b=60)]))
            
            conereimp.append(random.choice([0, "NA", random.randint(a=25, b=35)]))
            coneleimp.append(random.choice([0, "NA", random.randint(a=25, b=35)]))
            
            pvfre.append(random.choice(["yes", "no"]))
            pvfle.append(random.choice(["yes", "no"]))
            cvfre.append(random.choice(["yes", "no"]))
            cvfle.append(random.choice(["yes", "no"]))
            
            rcfr_iii.append(random.randint(a=25, b=35))
            rcfl_iii.append(random.randint(a=25, b=35))
            rcfr_v.append(random.randint(a=20, b=30))
            rcfl_v.append(random.randint(a=20, b=30))
                
    data = {"Patient ID": id_list, "Age": age, "REVA": reva, "LEVA": leva, "REref": reref, "LEref": leref,
            "RodRE": rodre, "RodLE": rodle, "MixREA": mixrea, "MixLEA": mixlea, "MixREB": mixreb, "MixLEB": mixleb,
            "ConeREamp": conereamp, "ConeLEamp": coneleamp, "ConeREimp": conereimp, "ConeLEimp": coneleimp,
            "PVFRE": pvfre, "PVFLE": pvfle, "CVFRE": cvfre, "CVFLE": cvfle, "RCFR_III": rcfr_iii, "RCFL_III": rcfl_iii,
            "RCFR_V": rcfr_v, "RCFL_V": rcfl_v}
    
    df = pd.DataFrame(data=data)

    return df

def generate_imaging_dataset():
    pass

def main():
    if len(sys.argv) < 5:
        print("Ussage: py BUZZI.py <num_rows> <output_file_name_1> <output_file_name_2> <output_file_name_3> <output_file_name_4>")
    else:
        num_rows = int(sys.argv[1])
        output_file_1 = sys.argv[2]
        output_file_2 = sys.argv[3]
        output_file_3 = sys.argv[4]
        output_file_4 = sys.argv[4]
        
        # Create the baseline clinical table
        fieldset = generate_baseline_clinical_dataset()
        hf = headfake.HeadFake.from_python({"fieldset":fieldset})
        data_1 = hf.generate(num_rows=num_rows)

        # Create CSV output
        data_1.to_csv(output_file_1, index=False)
        print(f"Baseline results written to {output_file_1}")
        
        # Create the genetic table
        data_2 = generate_genetic_dataset(data_1)
        
        # Create the CSV output
        data_2.to_csv(output_file_2, index=False)
        print(f"Diagnosis results written to {output_file_2}")
        
        # Create the dynamic table
        data_3 = generate_dynamic_dataset(data_1, 4)
        
        # Create the CSV output
        data_3.to_csv(output_file_3, index=False)
        print(f"Diagnosis results written to {output_file_3}")
        
        # Create the imaging data table
        #data_4 = generate_imaging_dataset()
        
        # Create the CSV output
        #data_4.to_csv(output_file_4, index=False)
        #print(f"Diagnosis results written to {output_file_4}")

if __name__ == "__main__":
    main()