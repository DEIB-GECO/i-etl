import headfake.field
import random

class MedicalHistory:
    def __init__(self):
        pass
    
    # Generate walk age in months
    def __generate_walk_age(walk_indep, *arguments):
        if walk_indep == "Yes":
            return f'{random.randint(a=9, b=48)} months'
        else:
            return "-"
        
     # Generate walk age in months
    def __generate_speak_age(speak_indep, *arguments):
        if speak_indep == "Yes":
            return f'{random.randint(a=12, b=65)} months'
        else:
            return "-"
    
    @staticmethod
    def generate_consanguinity(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_affected_relatives(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_walk_independently(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_walk_age(field_name, walk_indep):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=walk_indep, second_value=None, operator_fn=MedicalHistory.__generate_walk_age)
    
    @staticmethod
    def generate_speak_independently(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_speak_age(field_name, speak_indep):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=speak_indep, second_value=None, operator_fn=MedicalHistory.__generate_speak_age)
    
    @staticmethod
    def generate_developmental_disorder(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_intelectual_disability(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_autism(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_hiperactivity(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_language_absence(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    
    @staticmethod
    def generate_photo_available(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
    

class IDTest:
    __testName: None
    __testResult = None
    __description = None
    
    def __init__(self):
        pass
    
    @staticmethod
    def __generate_ID_test_to_string(hasID, *arguments):
        if hasID == "Yes":
            testResult = random.randint(a=10, b=70)
            return f'IQ {testResult}'
        elif hasID == "No information":
            return None
        else:
            testResult = random.randint(a=71, b=100)
            return f'IQ {testResult}'
    
    @staticmethod
    def generate_ID_test_to_string(field_name, hasID):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=hasID, second_value=None, operator_fn=IDTest.__generate_ID_test_to_string)