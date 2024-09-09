import random

class Karyotype:
    def __init__(self):
        pass
    
    
    # Generates karyotype results based on sex
    @staticmethod
    def generate_karyotype_result(sex=str, *arguments):
        if sex == "1": # Male
            random.choices(["46, XY", "47,XY,+21", "46,XY,del(1)(p36)", "46,XY,del(18)(q21.3q33)"])
        elif sex == "2": # Female
            random.choices(["46, XX", "47,XX,+21", "46,XX,del(1)(p36)", "46,XX,del(18)(q21.3q33)"])
        else:
            random.choices(["46, XY", "46, XX"])

class Microarray:
    def __init__(self):
        pass
    
    @staticmethod
    def get_microarray_significance(microarray_result=str, *arguments):
        if microarray_result == "positive":
            return random.choice(["Pathogenic", "Likely pathogenic"])
        elif microarray_result == "negative":
            return random.choice(["Benign", "Likely benign"])
        else:
            return "VUS"
    
    @staticmethod
    def generate_cytogenetic_location(*arguments):
        cytogenetic_locations_list = ["16q11.3", "12p6.2", "3q9.4", "22p4.3", "17q11.5", "2p4.3"]
        return random.choice(cytogenetic_locations_list)
    
    @staticmethod
    def generate_microarray_details(region, type):
        return f"The identified {region} {type} is frequently associated with developmental and neurobehavioral disorders, including intellectual disability."

    @staticmethod
    def generate_microarray_length(*arguments):
        return f"{random.randint(a=100, b=999)} kb"      

