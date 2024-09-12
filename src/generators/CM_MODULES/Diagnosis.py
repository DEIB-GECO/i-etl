import pandas as pd
import re
import headfake.field

df_genes_to_phenotype = pd.read_csv('DATA/genes_to_phenotype.txt', sep="\t")
df_countries = pd.read_csv('DATA/countries.csv', sep=",")
SJD_df_diseases = pd.read_csv('DATA/SJD_Diseases.csv', sep="\t")
# df_diseases = pd.read_csv('DATA/IMGGE_Diseases.csv', sep=",")

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
    def get_symptoms_by_omim_id(omim_id=int, symptoms_max_number=int):
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
    
    @staticmethod
    def generate_diagnosis_name(field_name, disease_file_name, key_field):
        return headfake.field.MapFileField(mapping_file=disease_file_name, key_field=key_field, name=field_name)
    
    @ staticmethod
    def get_symptoms_by_orphanet_id(orpha_id, symptoms_max_number=int):
            x = df_genes_to_phenotype.loc[df_genes_to_phenotype['disease_id'] == orpha_id]
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
    
    @staticmethod
    def __get_facial_symptoms(orpha_id, symptoms_max_number=int):
        x = df_genes_to_phenotype.loc[(df_genes_to_phenotype['disease_id'] == orpha_id) & (df_genes_to_phenotype['is_facial'] == 1)]
        result = []
        
        for i, row in x.iterrows():
            hpo_name = row["hpo_name"]
            result.append(hpo_name)
        
        if len(result) > symptoms_max_number:
            result = result[1:symptoms_max_number]
        
        return ','.join(result)
    
    @staticmethod
    def get_symptoms(field_name, omim_id=None, orpha_id=None, symptoms_max_number=5):
       if omim_id != None:
           return headfake.field.OperationField(name=field_name, operator=None, first_value=omim_id, second_value=symptoms_max_number, operator_fn=Diagnosis.get_symptoms_by_omim_id) 
       elif orpha_id != None:
           return headfake.field.OperationField(name=field_name, operator=None, first_value=orpha_id, second_value=symptoms_max_number, operator_fn=Diagnosis.get_symptoms_by_orphanet_id) 
       else:
           return "Disease not found"
       
    @staticmethod
    def __get_facial_hpo(symptoms_list, *arguments):
        symptoms = symptoms_list.split(",")
        result = []
        for symptom in symptoms:
            x = df_genes_to_phenotype.loc[(df_genes_to_phenotype['hpo_name'] == symptom)]
        
            for i, row in x.iterrows():
                hpo = row["hpo_id"]
                result.append(f"{symptom} {hpo}")
                break
                
        return ','.join(result)       
        
        
    @staticmethod
    def get_facial_symptoms(field_name, orpha_id, symptoms_max_number=5):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=orpha_id, second_value=symptoms_max_number, operator_fn=Diagnosis.__get_facial_symptoms) 
    
    @staticmethod
    def get_facial_HPO(field_name, symptoms_list):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=symptoms_list, second_value=None, operator_fn=Diagnosis.__get_facial_hpo)