import sys
import headfake
import headfake.field
import headfake.transformer
import scipy
import operator
import pandas as pd
import random
import sys
import datetime

import scipy.stats

from CM_MODULES.Patient import Patient, BirthData, GeographicData
from CM_MODULES.MetabolicTest import MetabolicTest, Measure
from CM_MODULES.Diagnosis import Diagnosis

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


def generate_genetic_dataset():
    pass

def generate_dynamic_dataset():
    pass

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
        #data_2 = generate_genetic_dataset()
        
        # Create the CSV output
        #data_2.to_csv(output_file_2, index=False)
        #print(f"Diagnosis results written to {output_file_2}")
        
        # Create the dynamic table
        #data_3 = generate_dynamic_dataset()
        
        # Create the CSV output
        #data_3.to_csv(output_file_3, index=False)
        #print(f"Diagnosis results written to {output_file_3}")
        
        # Create the imaging data table
        #data_4 = generate_imaging_dataset()
        
        # Create the CSV output
        #data_4.to_csv(output_file_4, index=False)
        #print(f"Diagnosis results written to {output_file_4}")

if __name__ == "__main__":
    main()