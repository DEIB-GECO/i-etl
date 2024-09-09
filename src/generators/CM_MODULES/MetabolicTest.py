import datetime
import random
import pandas as pd

import headfake
import headfake.field
import headfake.transformer


df_measures = pd.read_csv('DATA/metabolic_measures_normal_ranges.csv', sep=";")

class MetabolicTest:
    def __init__(self):
        pass
    
    @ staticmethod
    def generate_sample_id(field_name, min_value, length):
        idGenerator = headfake.field.IncrementIdGenerator(min_value=min_value, length=length)
        sample_id = headfake.field.IdField(name=field_name, generator=idGenerator)
        return sample_id
    
    @staticmethod
    # Adds a ramdom number of days between 1 and 7 to a date
    def get_sample_date(date_start, *arguments):
        num_days = random.randint(a=1, b=7)
        start = None
        
        if isinstance(date_start, str):
            start = datetime.datetime.strptime(date_start, "%d/%m/%Y")
        else:
            start = date_start
 
        return start + datetime.timedelta(days=num_days)
    
    @staticmethod
    def is_too_young(date_of_birth, date_sample):
        dob = None
        if isinstance(date_of_birth, str):
            dob = datetime.datetime.strptime(date_of_birth, "%d/%m/%Y")
        else:
            dob = date_of_birth          
        
        difference = date_sample - dob
        
        if difference.days < 2:
            return "1"
        else:
            return "0"

    @staticmethod
    def generate_sample_quality(field_name, options):
        sq = headfake.field.OptionValueField(name=field_name, probabilities=options)
        return sq
    
    @staticmethod
    def generate_sample_date(field_name, start_date):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=start_date, second_value=None, operator_fn=MetabolicTest.get_sample_date)
    
    @staticmethod
    def too_young_sample(field_name, date_of_birth, date_sample_collected):
        return headfake.field.OperationField(name=field_name, operator = None, first_value=date_of_birth, second_value=date_sample_collected, operator_fn=MetabolicTest.is_too_young)

class Measure:
    def __init__(self):
        pass
    
    @staticmethod
    def generate_measure_value(measure_name, *arguments):
        x = df_measures.loc[df_measures['measure'] == measure_name]
        if len(x) > 0:
            for i, row in x.iterrows():
                min_value = row["min"]
                max_value = row["max"]
                return round(random.uniform(a=min_value, b=max_value), 2)
        else:
            return "Measure not supported"
    
        