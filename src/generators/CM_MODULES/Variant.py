import pandas as pd

class Variant:
    def __init__(self):
        pass
    
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
