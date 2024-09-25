import sys
import pandas as pd
import random
import numpy as np
import json

from numpy.random import choice
from datetime import datetime
from CM_MODULES.Patient import Patient, BirthData
from faker import Faker

METADATA_FILE = "METADATA/UC3_METADATA_2.csv"
faker = Faker()


def get_categories(field_metadata):
    # Read categories represented as a JSON text in the metadata
    json_values = json.loads(field_metadata['JSON_values']) 
    
    categories = []
    for value in json_values:
        categories.append(value['value'])

    return categories

def get_probabilities(field_metadata):
    if pd.isna(field_metadata['Probabilities']):
        return None
    else:
        # Read probabilities represented as a JSON text in the metadata
        json_values = json.loads(field_metadata['Probabilities']) 
    
        probabilities = list(json_values.values())
    
        return probabilities

def get_prefix(field_metadata):
    if pd.isna(field_metadata['prefix']):
        return None
    else:
        return field_metadata['prefix']
    
def get_date_properties(field_metadata, row):
    min = field_metadata['Min']
    max = field_metadata['Max']
    date_properties = {"start":None, "end":None, "format":None}
    
    if pd.isna(min):
        date_properties['start'] = None
    else:
        if '{' in min:
            min_field = min.replace('{','').replace('}','')
            date_properties['start'] = datetime.strptime(row[min_field], field_metadata['Format'])
        else:
            date_properties['start'] = datetime.strptime(field_metadata['Min'], field_metadata['Format'])
        
    if pd.isna(field_metadata['Max']):
        date_properties['end'] = datetime.now()
    else:
        if '{' in max:
            max_field = max.replace('{','').replace('}','')
            date_properties['start'] = datetime.strptime(row[max_field], field_metadata['Format'])
        else:
            date_properties['end'] = datetime.strptime(field_metadata['Max'], field_metadata['Format'])
        
    if pd.isna(field_metadata['Format']):
        date_properties['format'] = None
    else:
        date_properties['format'] = field_metadata['Format']
    
    return date_properties
        
def generate_categorical_value(categories, probabilities):    
    if probabilities != None:
        return random.choice(categories)
    else:
        return choice(categories, 1, p=probabilities)[0]

def generate_ID_value(prefix, num_rows):
    id = random.randint(a=0, b=num_rows*1000)
    if prefix is None:
        return f'{id}'
    else:
        return f'{prefix}{id}' 

def generate_date(start_date, end_date, format):
    start_date = datetime.strptime(start_date, format)
    end_date = datetime.strptime(end_date, format)
    
    date = faker.date_between(start_date, end_date)
    
    return date.strftime(format)

def generate_int(min, max):
    return random.randint(a=min, b=max)

def generate_children(dob, dob_format, min, max):
    age = generate_age(dob_format, dob)
    
    if age >= 18:
        return generate_int(min, max)
    else:
        return 0    

def generate_age(birth_date_format, birth_date):
    dob = datetime.strptime(birth_date, birth_date_format)
    return datetime.now().year-dob.year

def generate_weight(date, date_format):
    dt = datetime.strptime(date, date_format)
    age = datetime.now().year - dt.year
    
    if age < 1:
        return random.randint(a=300, b=800)
    elif age == 1:
        return random.randint(a=800, b=1200)
    elif age == 2:
        return random.randint(a=1000, b=1500)
    elif age == 3:
        return random.randint(a=1100, b=1700)
    elif age == 4:
        return random.randint(a=1300, b=2000)
    elif age == 5:
        return random.randint(a=1500, b=2200)
    elif age >= 6 and age <= 8:
        return random.randint(a=1800, b=2700)
    elif age >= 9 and age <= 12:
        return random.randint(a=2700, b=4500)
    elif age >= 13 and age <= 15:
        return random.randint(a=4100, b=6400)
    elif age >= 16 and age <= 18:
        return random.randint(a=4700, b=8200)
    elif age >= 19 and age <= 24:
        return random.randint(a=5400, b=8600)
    elif age >=25 and age <= 34:
        return random.randint(a=5700, b=9100)
    elif age >= 35:
        return random.randint(a=5700, b=9300)

def generate_height(date, date_format):
    dt = datetime.strptime(date, date_format)
    age = datetime.now().year - dt.year
    
    if age < 1:
        return round(random.randint(a=47, b=54), 0)
    elif age == 2:
        return round(random.randint(a=80, b=90), 0)
    elif age == 3:
        return round(random.randint(a=91, b=98),0)
    elif age == 4:
        return round(random.randint(a=99, b=105), 0)
    elif age == 5:
        return round(random.randint(a=106, b=113), 0)
    elif age == 6:
        return round(random.randint(a=114, b=120), 0)
    elif age == 7:
        return round(random.randint(a=121, b=127), 0)
    elif age == 8:
        return round(random.randint(a=128, b=132), 0)
    elif age == 9:
        return round(random.randint(a=133, b=138), 0)
    elif age == 10:
        return round(random.randint(a=139, b=143), 0)
    elif age == 11:
        return round(random.randint(a=144, b=148), 0)
    elif age == 12:
        return round(random.randint(a=149, b=154), 0)
    elif age == 13:
        return round(random.randint(a=155, b=160), 0)
    elif age >= 14:
        return round(random.randint(a=160, b=182), 0)

def generate_test_value(categories):
    return random.choice(categories)
    
def get_parameters(parameters_field):
    if pd.isna(parameters_field):
        return None
    else:
        return json.loads(parameters_field)

def get_parameter(parameter_value, field_list, synthetic_values):
    if parameter_value in field_list:
        if len(synthetic_values) > 0:
            return synthetic_values[parameter_value]
    return parameter_value

def generate_field_value(field_list, field_metadata, num_rows, synthetic_values, mapping_values, dataset_name, value_index):
    var_type = field_metadata['Synthetic Vartype']
    parameters = get_parameters(field_metadata['GeneratorParameters']) 
    
    if var_type == 'Categorical':
        categories = get_categories(field_metadata)
        probabilities = get_probabilities(field_metadata)
        return generate_categorical_value(categories, probabilities)
    
    elif var_type == 'ID':
        prefix = get_parameter(parameters['prefix'], field_list, synthetic_values)
        return generate_ID_value(prefix, num_rows)
    
    elif var_type == "Date":
        start_date = get_parameter(parameters['start_date'], field_list, synthetic_values)
        end_date = get_parameter(parameters['end_date'], field_list, synthetic_values)
        format = get_parameter(parameters['format'], field_list, synthetic_values)
        return generate_date(start_date, end_date, format)
    
    elif var_type == "INT":
        min = get_parameter(parameters['min'], field_list, synthetic_values)
        max = get_parameter(parameters['max'], field_list, synthetic_values)
        return generate_int(min, max)
    
    elif var_type == "CHILDREN":
        dob = get_parameter(parameters['dob'], field_list, synthetic_values)
        dob_format = get_parameter(parameters['dob_format'], field_list, synthetic_values)
        min = get_parameter(parameters['min'], field_list, synthetic_values)
        max = get_parameter(parameters['max'], field_list, synthetic_values)
        return generate_children(dob, dob_format, min, max)
    
    elif var_type == "AGE":
        dob_format = get_parameter(parameters['format'], field_list, synthetic_values)
        dob = get_parameter(parameters['start_date'], field_list, synthetic_values)
        return generate_age(dob_format, dob)
    
    elif var_type == "WEIGHT":
        visit_date = get_parameter(parameters['date'], field_list, synthetic_values)
        date_format = get_parameter(parameters['format'], field_list, synthetic_values)
        return generate_weight(visit_date, date_format)
    
    elif var_type == "HEIGHT":
        visit_date = get_parameter(parameters['date'], field_list, synthetic_values)
        date_format = get_parameter(parameters['format'], field_list, synthetic_values)
        return generate_height(visit_date, date_format)
    
    elif var_type == "TEST":
        categories = get_categories(field_metadata)
        return generate_test_value(categories)
    
    elif var_type == "MAPPING":
        mapping_field = get_parameter(parameters['mapping_field'], field_list, synthetic_values)
        mapping_dataset = get_parameter(parameters['dataset'], field_list, synthetic_values)
        for mapping_element in mapping_values:
            if mapping_element['dataset'] == mapping_dataset and mapping_element['field_name'] == mapping_field:
                values = mapping_element['data']
                return values[value_index]
        return None
    
    return None

def get_mapping_values(synthetic_data, mapping_fields, dataset_name):
    mapping_fields_names = list(mapping_fields['name'])
    results = []
    for field in mapping_fields_names:
        field_data = list(synthetic_data[field])
        results.append({"dataset":dataset_name, "field_name":field, "data":field_data})
    return results
    
def generate_synthetic_data(metadata, dataset_name, num_rows, mapping_values):
    # Filter rows by dataset name
    subset = metadata.loc[metadata['dataset'] == dataset_name]
    field_list = list(subset.loc[:, 'name'].values)
    
    synthetic_data = []
    
    # Populate the synthetic dataset row by row
    for i in range(num_rows):
        synthetic_values = {}
        
        # Generate field values
        for j, row in subset.iterrows():
            field_name = row['name']
            synthetic_values[field_name] = generate_field_value(field_list, row, num_rows, synthetic_values, mapping_values, dataset_name, i)

        # Append new row data
        synthetic_data.append(synthetic_values)

    # Create the synthetic DataFrame
    synthetic_dataframe = pd.DataFrame(synthetic_data, columns=subset['name'])
            
    return synthetic_dataframe
        

def main():
    if len(sys.argv) < 2:
        print("Ussage: py UC3.py <num_rows>")
    else:
        num_rows = int(sys.argv[1])
        
        # Read metadata file
        metadata_df = pd.read_csv(METADATA_FILE, sep=';')
        
        mapping_fields = metadata_df.loc[metadata_df['IsMappingField'] == 1]
        mapping_values = []
        
        # Get the names of the resulting datasets
        dataset_names = np.unique(metadata_df['dataset'])
        
        for dataset in dataset_names:
            synthetic_df = generate_synthetic_data(metadata_df, dataset, num_rows, mapping_values)
            synthetic_df.to_csv(dataset, index=False)
            print(f"Results written to {dataset}")
            
            # Store generated values that act as mappings to other datasets
            df_mapping_fields = mapping_fields.loc[mapping_fields['dataset'] == dataset] # Get the mapping fields of current dataset metadata
            df_mapping_values = get_mapping_values(synthetic_df, df_mapping_fields,dataset)
            mapping_values = mapping_values + df_mapping_values # Get the generated values of the mapping fields       

if __name__ == "__main__":
    main()