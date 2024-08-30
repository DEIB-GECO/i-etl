import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize number of patients per disease
PATIENTS_PER_DISEASE = 10

# Initialize Faker and seed for reproducibility
fake = Faker('it_IT')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Disease list
diseases = [
    "Phenylketonuria", "Benign hyperphenylalaninemia", "Deficiency of the biosynthesis of the biopterin cofactor", 
    "Deficit in the regeneration of the biopterin cofactor", "Tyrosinemia type I", "Tyrosinemia type II", 
    "Maple syrup urine disease", "Homocystinuria (CBS defect)", "Homocystinuria (severe MTHFR defect)", 
    "Glutaric acidemia type I", "Isovaleric acidemia", "Beta ketothiolase deficiency", 
    "3-hydroxy 3-methyl glutaric acidemia", "Propionic acidemia", "Methylmalonic acidemia (Mut)", 
    "Methylmalonic acidemia (Cbl-A)", "Methylmalonic acidemia (Cbl-B)", 
    "Methylmalonic acidemia with homocystinuria (Cbl C deficiency)", 
    "Methylmalonic acidemia with homocystinuria (Cbl D deficiency)", "2-methyl butyryl-CoA dehydrogenase deficiency", 
    "Malonic aciduria", "Multiple carboxylase deficiency", "Citrullinemia type I", 
    "Citrullinemia type II (Citrine deficiency)", "Argininosuccinic acidemia", "Argininemia", 
    "Carnitine transport deficiency", "Carnitine palmitoyl transferase deficiency", 
    "Carnitine-acylcarnitine translocase deficiency", "Carnitine palmitoyl transferase deficiency", 
    "Very long-chain acyl-CoA dehydrogenase deficiency", "Mitochondrial trifunctional protein deficiency", 
    "Long-chain 3-hydroxy-acyl-CoA dehydrogenase deficiency", "Medium-chain acyl-CoA dehydrogenase deficiency", 
    "Medium/short-chain 3-hydroxy-acyl-CoA dehydrogenase deficiency", "Glutaric acidemia type II"
]

# Main Italian cities
cities = ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence", "Bari", "Catania"]

# Ethnic distribution (simplified for example purposes)
ethnicities = ["European", "African", "Asian", "Latin American", "Other"]
ethnicity_weights = [0.94, 0.02, 0.02, 0.01, 0.01]

# Main Italian hospitals
hospitals = [
    "Policlinico Universitario A. Gemelli", "Ospedale San Raffaele", "Ospedale Niguarda Ca' Granda", "Ospedale San Giovanni Battista", 
    "Ospedale Maggiore Policlinico", "Ospedale Pediatrico Bambino Gesù", "Istituto Clinico Humanitas", "Ospedale Policlinico San Matteo",
    "Azienda Ospedaliera Universitaria Careggi", "Ospedale Sant'Andrea"]

# Data columns
columns = [
    "Id", "SampleBarcode", "Ethnicity", "GestationalAge", "City", "Sex", "DateOfBirth", "Weight", "AntibioticsBaby",
    "AntibioticsMother", "Meconium", "CortisoneBaby", "CortisoneMother", "ThyroidMother", "Premature", "BabyFed", 
    "HUFeed", "MixFeed", "ArtFeed", "TPNFeed", "EnteralFeed", "TPNCARNFeed", "TPNMCTFeed", "BirthMethod", "Twins", 
    "synthetic_dataset_disease", "SampleQuality", "SamTimeCollected", "SamTimeReceived", "Hospital", "AnswerIX", 
    "TooYoung", "Sampling", "BIS", "ASATotal", "Ala", "Arg", "Cit", "Glu", "Gly", "Le/Ile/Pro-OH", "MET", "Orn", "PHE", "Pro", 
    "TYR", "Val", "BTD", "C0", "C2", "C3", "C3DC/C4OH", "C4", "C4DC/C5OH", "C5", "C5:1", "C5DC/C6OH", "C6", 
    "C6DC", "C8", "C8:1", "C10", "C10:1", "C10:2", "C12", "C12:1", "C14", "C14:1", "C14:2", "C14-OH", "C16", 
    "C16:OH", "C16:1-OH", "C18", "C18:1", "C18:2", "C18-OH", "C18:1-OH", "C18:2-OH", "C20", "C22", "C24", "C26", 
    "SA", "ADO", "D-ADO", "C20:0-LPC", "C22:0-LPC", "C24:0-LPC", "C26:0-LPC", "s-TSH", "IRT-GSP", "TGAL", "MMA", 
    "EMA", "GA", "2OH GA", "3OH GA", "HCYs", "3OH PA", "MCA", "OROTICO", "PIVA", "2MBC", "c4-b", "c4-i", "Allele 1", 
    "Allele 2"
]

# Generate sample barcode
def generate_barcode():
    return fake.bothify(text='???# ###', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')

# Generate gestational age
def generate_gestational_age():
    return random.randint(24, 42)

# Generate birth weight
def generate_weight():
    return random.randint(1000, 5000)

# Generate premature
def generate_premature(gestational_age):
    if gestational_age < 37:
        return 1
    else:
        return random.choice(['0','NA'])
    
# Generate too young
def generate_too_young(birth_date, sam_time_collected):
    days = (sam_time_collected-birth_date).days
    if days <= 2:
        return 1
    else:
        return 0

# Generate boolean column with NA option
def generate_bool_with_na():
    return random.choice(['0', '1', 'NA'])

# Generate metabolic measure
def generate_metabolic_measure(mean, std):
    return round(np.random.normal(mean, std), 2)

# Generate date within a range
def generate_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def generate_allele_1():
    alleles = ["A", "C", "T", "G","NA"]
    allele_weights = [0.025, 0.025, 0.025, 0.025, 0.9]
    return np.random.choice(alleles, p=allele_weights)

def generate_allele_2(allele_1):
    alleles = ["A", "C", "T", "G"]
    allele_weights = [0.025, 0.025, 0.025, 0.9]
    p = np.array(allele_weights)
    p /= p.sum()

    if allele_1 == "NA":
        return "NA"
    else:
        return np.random.choice(alleles, p=p)
    
def generate_sampling():
    sampling = ["ADDA", "Basale già", "Controllo", "Iniziale"]
    sampling_weights = [0.005, 0.015, 0.08, 0.9]
    return np.random.choice(sampling, p=sampling_weights)

def generate_answer_IX():
    answer_IX = ["1", "2", "3", "4"]
    answer_IX_weights = [0.9, 0.08, 0.015, 0.005]


existing_patient_ids = {}  # there may be several samples for a single patient
def generate_patient_id(sample_barcode, sample_type: str):
    if sample_type == "Controllo" and sample_barcode in existing_patient_ids:
        # this is another sample for an existing patient, and we already encountered that patient
        return existing_patient_ids[sample_barcode]
    else:
        # this is a sample for a new patient
        random_pid = np.random.randint(0, 100000000)
        while random_pid in existing_patient_ids.values():
            random_pid = np.random.randint(0, 100000000)
        existing_patient_ids[sample_barcode] = random_pid
        return random_pid


# Generate the synthetic dataset
data = []

for disease in diseases:
    for i in range(PATIENTS_PER_DISEASE):
        base_date = datetime.now() - timedelta(days=random.randint(0, 365*5))  # base date within the last 5 years
        birth_date = generate_date(base_date - timedelta(days=365*5), base_date)
        
        # First screening
        sample_collected_date_1 = generate_date(birth_date + timedelta(days=1), birth_date + timedelta(days=60))
        sample_received_date_1 = sample_collected_date_1 + timedelta(days=random.randint(1, 3))
        
        for sample_num, (sample_collected_date, sample_received_date) in enumerate([(sample_collected_date_1, sample_received_date_1)]):
            gestational_age = generate_gestational_age()
            sam_time_collected = fake.date_time_between(start_date=birth_date, end_date=birth_date + timedelta(days=5))
            sam_time_received = fake.date_time_between(start_date=sam_time_collected, end_date=sam_time_collected + timedelta(days=3))
            allele_1 = generate_allele_1()
            allele_2 = generate_allele_2(allele_1)
            barcode = generate_barcode()
            sample_type = generate_sampling()
            row = {
                "Id": generate_patient_id(sample_barcode=barcode, sample_type=sample_type),
                "SampleBarcode": barcode,
                "Ethnicity": np.random.choice(ethnicities, p=ethnicity_weights),
                "GestationalAge": generate_gestational_age(),
                "City": random.choice(cities),
                "Sex": random.choice(['M', 'F']),
                "DateOfBirth": birth_date.strftime('%Y/%m/%d'),
                "Weight": generate_weight(),
                "AntibioticsBaby": generate_bool_with_na(),
                "AntibioticsMother": generate_bool_with_na(),
                "Meconium": generate_bool_with_na(),
                "CortisoneBaby": generate_bool_with_na(),
                "CortisoneMother": generate_bool_with_na(),
                "ThyroidMother": generate_bool_with_na(),
                "Premature": generate_premature(gestational_age),
                "BabyFed": generate_bool_with_na(),
                "HUFeed": generate_bool_with_na(),
                "MixFeed": generate_bool_with_na(),
                "ArtFeed": generate_bool_with_na(),
                "TPNFeed": generate_bool_with_na(),
                "EnteralFeed": generate_bool_with_na(),
                "TPNCARNFeed": generate_bool_with_na(),
                "TPNMCTFeed": generate_bool_with_na(),
                "BirthMethod": random.choice(['naturale', 'cesareo', 'altro']),
                "Twins": generate_bool_with_na(),
                "synthetic_dataset_disease": disease,
                "SampleQuality": random.choice(['inadeguata', 'sufficiente', 'ok']),
                "SamTimeCollected": sam_time_collected.strftime("%Y/%m/%d %H:%M:%S"),
                "SamTimeReceived": sam_time_received.strftime("%Y/%m/%d %H:%M:%S"),
                "Hospital": random.choice(hospitals),
                "AnswerIX": fake.random_int(min=1,max=4),
                "TooYoung": generate_too_young(birth_date, sam_time_collected),
                "Sampling": sample_type,
                "BIS": generate_metabolic_measure(50, 10),
                "ASATotal": generate_metabolic_measure(30, 5),
                "Ala": generate_metabolic_measure(25, 4),
                "Arg": generate_metabolic_measure(15, 3),
                "Cit": generate_metabolic_measure(12, 2),
                "Glu": generate_metabolic_measure(60, 10),
                "Gly": generate_metabolic_measure(200, 40),
                "Le/Ile/Pro-OH": generate_metabolic_measure(30, 6),
                "MET": generate_metabolic_measure(20, 5),
                "Orn": generate_metabolic_measure(10, 2),
                "PHE": generate_metabolic_measure(5, 1),
                "Pro": generate_metabolic_measure(50, 10),
                "TYR": generate_metabolic_measure(40, 8),
                "Val": generate_metabolic_measure(60, 12),
                "BTD": generate_metabolic_measure(0.5, 0.1),
                "C0": generate_metabolic_measure(40, 8),
                "C2": generate_metabolic_measure(25, 5),
                "C3": generate_metabolic_measure(12, 3),
                "C3DC/C4OH": generate_metabolic_measure(1, 0.2),
                "C4": generate_metabolic_measure(1.5, 0.3),
                "C4DC/C5OH": generate_metabolic_measure(0.8, 0.2),
                "C5": generate_metabolic_measure(1.2, 0.3),
                "C5:1": generate_metabolic_measure(0.4, 0.1),
                "C5DC/C6OH": generate_metabolic_measure(0.5, 0.1),
                "C6": generate_metabolic_measure(0.6, 0.2),
                "C6DC": generate_metabolic_measure(0.4, 0.1),
                "C8": generate_metabolic_measure(0.8, 0.2),
                "C8:1": generate_metabolic_measure(0.3, 0.1),
                "C10": generate_metabolic_measure(0.7, 0.2),
                "C10:1": generate_metabolic_measure(0.2, 0.1),
                "C10:2": generate_metabolic_measure(0.1, 0.1),
                "C12": generate_metabolic_measure(0.9, 0.2),
                "C12:1": generate_metabolic_measure(0.4, 0.1),
                "C14": generate_metabolic_measure(1, 0.3),
                "C14:1": generate_metabolic_measure(0.6, 0.2),
                "C14:2": generate_metabolic_measure(0.5, 0.2),
                "C14-OH": generate_metabolic_measure(0.3, 0.1),
                "C16": generate_metabolic_measure(1.2, 0.3),
                "C16:OH": generate_metabolic_measure(0.7, 0.2),
                "C16:1-OH": generate_metabolic_measure(0.6, 0.2),
                "C18": generate_metabolic_measure(1.4, 0.4),
                "C18:1": generate_metabolic_measure(0.8, 0.3),
                "C18:2": generate_metabolic_measure(0.5, 0.2),
                "C18-OH": generate_metabolic_measure(0.2, 0.1),
                "C18:1-OH": generate_metabolic_measure(0.3, 0.1),
                "C18:2-OH": generate_metabolic_measure(0.2, 0.1),
                "C20": generate_metabolic_measure(0.1, 0.1),
                "C22": generate_metabolic_measure(0.1, 0.1),
                "C24": generate_metabolic_measure(0.1, 0.1),
                "C26": generate_metabolic_measure(0.1, 0.1),
                "SA": generate_metabolic_measure(0.2, 0.1),
                "ADO": generate_metabolic_measure(0.1, 0.1),
                "D-ADO": generate_metabolic_measure(0.1, 0.1),
                "C20:0-LPC": generate_metabolic_measure(0.1, 0.1),
                "C22:0-LPC": generate_metabolic_measure(0.1, 0.1),
                "C24:0-LPC": generate_metabolic_measure(0.1, 0.1),
                "C26:0-LPC": generate_metabolic_measure(0.1, 0.1),
                "s-TSH": generate_metabolic_measure(2, 1),
                "IRT-GSP": generate_metabolic_measure(70, 10),
                "TGAL": generate_metabolic_measure(100, 20),
                "MMA": generate_metabolic_measure(2, 0.5),
                "EMA": generate_metabolic_measure(0.5, 0.1),
                "GA": generate_metabolic_measure(0.8, 0.2),
                "2OH GA": generate_metabolic_measure(0.5, 0.1),
                "3OH GA": generate_metabolic_measure(0.4, 0.1),
                "HCYs": generate_metabolic_measure(10, 2),
                "3OH PA": generate_metabolic_measure(0.3, 0.1),
                "MCA": generate_metabolic_measure(0.2, 0.1),
                "OROTICO": generate_metabolic_measure(0.1, 0.1),
                "PIVA": generate_metabolic_measure(0.2, 0.1),
                "2MBC": generate_metabolic_measure(0.3, 0.1),
                "c4-b": generate_metabolic_measure(0.4, 0.1),
                "c4-i": generate_metabolic_measure(0.2, 0.1),
                "Allele 1": allele_1,
                "Allele 2": allele_2
            }
            
            data.append(row)

# Convert to DataFrame
df = pd.DataFrame(data, columns=columns)

# Save to CSV
output_path = "../../datasets/data/BUZZI/generated-screening.csv"
print(output_path)
df.to_csv(output_path, index=False)

