import pandas as pd
import re

df_genes_to_phenotype = pd.read_csv('DATA/genes_to_phenotype.txt', sep="\t")
df_countries = pd.read_csv('DATA/countries.csv', sep=",")
df_diseases = pd.read_csv('DATA/IMGGE_Diseases.csv', sep=",")

class Diagnosis:
    def __init__(self):
        pass

    # Removes blank spaces and transforms # prefix into OMIM:
    @staticmethod
    def transform_omimID(omimID=str, *arguments):
        if omimID != None:
            return omimID.replace("# ", "OMIM:")
        else:
            return omimID

    # Gets the list of symptoms and HPO identifiers as a formated string
    @ staticmethod
    def get_symptoms(omim_id=int, symptoms_max_number=int):
        if omim_id != None:
            omim_id_list = omim_id.split(", ") # There cane be more than one OMIM ID if phenotype is a disease set
            x = df_genes_to_phenotype.loc[df_genes_to_phenotype['disease_id'] == omim_id_list[0]] # We compare with the first OMIM ID

            result = []

            for i, row in x.iterrows():
                hpo = row["hpo_id"]
            
                # Do not consider inheritance hpo identifiers
                if hpo not in ["HP:0000007","HP:0000006","HP:0001417","HP:0001419","HP:0001423","HP:0001426","HP:0001427","HP:0001450","HP:0012275","HP:0010984","HP:0010983","HP:0010982"]:
                    hpo_name = row["hpo_name"]
                    result.append(f"{hpo_name} {hpo}")

                if len(result) > symptoms_max_number:
                    result = result[1:symptoms_max_number]
            
            return ','.join(result)
        else:
            return None
    
    # Gets the list of HPO identifiers as a formated string
    @staticmethod
    def get_hpo_identifiers(symptoms=str, *arguments):
        if symptoms != None:
            symptoms_list = symptoms.split(",")
            result = []
            for symptom in symptoms_list:
                m = re.search('HP:[0-9]+', symptom)
                if m:
                    result.append(m.group(0))
            return ", ".join(result)
        return None