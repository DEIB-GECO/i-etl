import pandas as pd
import headfake.field
import random


class Variant:
    start: None
    end: None
    ref: None
    alt: None
    type: None
    zygosity: None
    coding_name: None
    protein_name: None
    segregation: None
    zygosity2var: None
    
    def __init__(self, start, end, ref, alt, type, zygosity, coding_name, protein_name, segregation, zygosity2var):
        self.start = start
        self.end = end
        self.ref = ref
        self.alt = alt
        self.type = type
        self.zygosity = zygosity
        self.coding_name = coding_name
        self.protein_name = protein_name
        self.segregation = segregation
        self.zygosity2var = zygosity2var
    
    @staticmethod
    def get_protein_name(coding_name, *arguments):
        df = pd.read_csv('DATA/variants.csv', sep=",")
        x = df.loc[df['coding_name'] == coding_name]
    
        if len(x) == 0:
            return None

        protein_name = ""
        for i, row in x.iterrows():
            protein_name = row["protein_name"]
        
        return protein_name     
    
    @staticmethod
    def generate_zygosity(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def generate_start_nucleotide():
        return random.randint(a=12345, b=99999)
    
    def generate_end_nucleotide(start_nucleotide):
        i = random.randint(0,10)
        return start_nucleotide + i

    @staticmethod
    def generate_zygosity_2(options):
        return random.choice(list(options.keys()))


class VariantInheritance:
    def __init__(self):
        pass
    
    @staticmethod
    def generate_inheritance(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)
