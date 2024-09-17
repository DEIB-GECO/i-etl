import sys
import headfake
import headfake.field
import headfake.transformer
import random
import sys
import datetime
import pandas as pd

from CM_MODULES.Patient import Patient
from CM_MODULES.Diagnosis import Diagnosis

uc2_diseases_df = pd.read_csv('DATA/UC2_Diseases.csv', sep=',')
variants_df = pd.read_csv('DATA/variants.csv', sep=",")

ETHNICITIES = ["ASH","NA","O","Y","Et","S","B","BU","I","IS","M","MR","G","Gr","T","Eg","U","AM","B","AC"]
FUNDUS_DESCRIPTIONS = [
    "The optic disc is round with sharp margins and a pinkish hue. The cup-to-disc ratio is about 0.3. The retinal vessels have a regular caliber with no signs of narrowing or tortuosity. The macula appears flat, with a well-defined foveal reflex. The retinal background is orange-red without hemorrhages, exudates, or pigmentary changes.",
    "The optic disc margins are clear. Multiple microaneurysms, dot and blot hemorrhages, and scattered hard exudates are present in all quadrants. The macula shows mild retinal thickening and some exudates near the fovea, indicating macular edema. No neovascularization is noted.",
    "The optic disc margins are clear. There is generalized arteriolar narrowing, with arteriovenous nicking observed at several points. Flame-shaped hemorrhages and cotton-wool spots are seen in the superior and inferior temporal quadrants. No optic disc edema is present.",
    "The optic disc is normal in appearance. Multiple small and large drusen are present in the macular area. The retinal pigment epithelium shows areas of hyperpigmentation and atrophy. No signs of subretinal fluid or hemorrhage are seen.",
    "The optic disc is hyperemic and blurred. Widespread retinal hemorrhages are seen in all quadrants. Venous dilation and tortuosity are marked. Cotton-wool spots are scattered throughout the retina. Macular edema is present.",
    "The optic disc is swollen, with blurred and elevated margins. The cup is obscured due to the swelling. Venous congestion is noted, with dilated and tortuous veins. Peripapillary hemorrhages and cotton-wool spots are visible. The retinal background is normal apart from changes near the optic disc.",
    "The optic disc is normal. There is a grayish, elevated retinal fold in the superior temporal quadrant with a demarcation line. The detached retina appears undulating with visible retinal vessels. No tears or holes are directly visible, but the macula remains attached.",
    "The optic disc and retinal vessels appear normal. There is a dome-shaped, pigmented lesion in the inferior nasal quadrant. The lesion has irregular borders with associated retinal detachment. Overlying orange pigment and drusen are visible on the surface of the lesion.",
    "The optic disc appears pale with waxy atrophy. Arterioles are attenuated throughout the retina. There is bone-spicule pigmentation in the mid-peripheral retina. The macula appears normal, though the foveal reflex may be absent.",
    "The optic disc is normal. There is a well-demarcated, circular area of retinal elevation in the macular region, indicating subretinal fluid. The retinal pigment epithelium shows subtle changes, including pigment clumping and mottling. No hemorrhages, exudates, or signs of choroidal neovascularization are present."
]

WIDE_FIELD_COLOR = [
    "The optic disc is well-defined with a pinkish hue and a cup-to-disc ratio of 0.3. Retinal vessels have a regular caliber without signs of tortuosity or narrowing. The macula shows a normal foveal reflex, and the peripheral retina appears uniform with no lesions, hemorrhages, or pigmentary changes.",
    "Multiple dot and blot hemorrhages are present throughout the retina, most notably in the mid-periphery. Microaneurysms are scattered, along with patches of hard exudates. No neovascularization is observed. The optic disc margins are clear.",
    "The retinal arterioles are narrowed with areas of arteriovenous (AV) nicking. Cotton-wool spots and flame-shaped hemorrhages are visible in the superior and inferior temporal quadrants. The optic disc is normal, and the macula appears intact.",
    "A grayish, elevated retinal area is seen in the inferior temporal quadrant. The detached retina appears undulating with prominent retinal vessels. No visible retinal tears, but the optic disc and macula remain attached.",
    " The optic disc is hyperemic and obscured by hemorrhages. Diffuse, deep, and superficial retinal hemorrhages are present in all quadrants. The veins are dilated and tortuous."
    "The macula has a reddish-orange discoloration with subretinal hemorrhage and grayish-green choroidal neovascular membrane. The peripheral retina appears normal. Drusen and retinal pigment epithelial changes are noted near the macula.",
    "The optic disc appears pale with a waxy atrophy. The retinal vessels, particularly the arterioles, are markedly attenuated. Bone-spicule pigmentation is scattered throughout the mid-peripheral retina. The macula shows mild retinal pigment epithelial changes.",
    "The macula displays a round, well-circumscribed defect. There is no evidence of vitreomacular traction. The optic disc and the peripheral retina are unremarkable.",
    "A dome-shaped, pigmented lesion is seen in the superior nasal quadrant. The lesion has irregular borders, and there is associated retinal detachment around its margins. Overlying drusen and orange pigment can be observed.",
    "The superior temporal quadrant shows flame-shaped and blot hemorrhages confined to the distribution of the affected vessel. There is retinal edema and a few cotton-wool spots in the area of the occlusion. The optic disc and other quadrants appear normal."
]

WIDE_FIELD_AUTOFLUORESCENCE = [
    "The optic disc appears dark and non-autofluorescent. Retinal blood vessels are also dark, blocking the underlying fluorescence. The background retinal autofluorescence is relatively uniform with a slight decrease in the macular region due to macular pigments. No areas of abnormal hyper- or hypofluorescence are seen.",
    "Multiple areas of scattered hyperautofluorescence corresponding to drusen are present in the macular area. Surrounding these areas, there are patches of hypoautofluorescence indicating retinal pigment epithelium (RPE) atrophy. Some areas show a mottled pattern of mixed hyper- and hypoautofluorescence, suggestive of varying stages of RPE damage.",
    "There is a classic ring of hyperautofluorescence surrounding the macula, representing zones of degenerating RPE. Beyond this ring, extensive areas of hypoautofluorescence are seen in the mid-peripheral retina, indicating widespread RPE atrophy. The optic disc and retinal vessels show normal autofluorescence patterns.",
    "Focal areas of increased hyperautofluorescence are noted in the macular region, suggesting the accumulation of subretinal fluid or focal RPE changes. There may also be associated areas of granular hypoautofluorescence around the zones of leakage, indicating chronic RPE alterations.",
    "The macular region shows a diffuse pattern of mottled hyper- and hypoautofluorescence. The hypoautofluorescent areas correspond to RPE atrophy, while the flecks of hyperautofluorescence represent lipofuscin accumulation in the RPE cells. Some areas of the peripheral retina may remain relatively normal.",
    "Extensive areas of hypoautofluorescence in the peripheral retina represent choroidal and RPE atrophy. The macula might remain relatively spared, showing normal autofluorescence, while the optic disc also appears dark. A few patches of residual hyperautofluorescence may indicate surviving RPE islands.",
    "The macula shows a well-defined area of intense hyperautofluorescence, representing the vitelliform lesion. Surrounding this, there might be scattered spots of hypoautofluorescence if there is any RPE atrophy. The peripheral retina typically appears normal.",
    "Large patches of hypoautofluorescence in the macular region correspond to areas of geographic atrophy. Surrounding these patches, a rim of hyperautofluorescence may indicate zones of ongoing RPE stress and degeneration. The peripheral retina usually maintains a normal autofluorescence pattern.",
    "A characteristic ring of hyperautofluorescence surrounds the macula, resembling a bull's-eye pattern. The central macular region shows hypoautofluorescence, indicating RPE and photoreceptor cell damage. The peripheral retina typically appears unaffected.",
    "The macula demonstrates a large, central area of hypoautofluorescence, indicating cone cell and RPE atrophy. Surrounding this, there may be a ring of hyperautofluorescence, representing stressed RPE. The peripheral retina might show scattered patches of hypoautofluorescence if the rod cells are also affected."
]

CENTRAL_OPTICAL_COHERENCE = [
    "The retinal layers are well-defined, with a normal foveal depression. The retinal pigment epithelium (RPE) appears intact, and there is no evidence of subretinal fluid, intraretinal cysts, or macular thickening. Central retinal thickness is within the normal range (250–300 µm).",
    "The scan shows multiple cystic spaces within the inner retinal layers, particularly in the macular region. There is an increased central retinal thickness, often exceeding 300 µm. The cystoid spaces may appear as round or oval hyporeflective (dark) areas.",
    "A full-thickness defect is present in the central foveal area, often with a surrounding cuff of subretinal fluid. The edges of the retinal tissue may appear elevated. There is a loss of the normal foveal contour. In some cases, an operculum (a detached piece of retinal tissue) may be seen within the vitreous cavity.",
    "The OCT scan shows a hyperreflective membrane on the inner surface of the retina, causing retinal folds and distortion. The retinal layers beneath the membrane may appear thickened, and the foveal contour is typically disturbed.",
    "The OCT shows multiple small, hyperreflective deposits beneath the RPE in the macular area. The RPE layer appears undulating due to the presence of drusen. There is no evidence of subretinal fluid or intraretinal cysts.",
    "The scan reveals a hyperreflective area corresponding to neovascular membranes. There may be subretinal or intraretinal fluid, indicating leakage. The RPE may appear disrupted or elevated. Increased retinal thickness and cystoid spaces might be present in the macular region.",
    "A dome-shaped detachment of the neurosensory retina is visible in the macular area. There is clear subretinal fluid accumulation without significant retinal thickening. The RPE layer may appear altered or disrupted in the region of the detachment.",
    "The scan shows diffuse retinal thickening with multiple hyporeflective (dark) cystoid spaces within the retina, typically in the inner nuclear and outer plexiform layers. The central retinal thickness is often increased beyond the normal range. Subretinal fluid may also be present.",
    " The vitreous appears partially attached to the macula, exerting traction on the retinal surface. The retinal contour is distorted, often with a stretched or elevated foveal area. There may be cystoid spaces indicating early cystoid macular edema.",
    "The OCT shows areas of RPE thinning and atrophy, often with an abrupt loss of retinal layers. The choroidal structures may appear more prominent due to the loss of overlying retinal tissue. There is no evidence of subretinal fluid or cystoid changes."
]

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


def generate_genetic_dataset(baseline_df):
    patient_id = baseline_df["Patient ID"]
    inheritance = []
    gene = []
    mutation1 = []
    mutation2 = []

    for id in patient_id:
        inheritance.append(random.choice(["AR","AD","XL"]))
        
        def get_gene(disease):
            x1 = uc2_diseases_df.loc[uc2_diseases_df['DiseaseName'] == disease]
            genes = None
            for i, row in x1.iterrows():
                genes = row["AffectedGenes"]
                break
            gene_list = genes.split(',')
        
            return random.choice(gene_list)
        
        disease = baseline_df.loc[baseline_df['Patient ID'] == id]['Dx'].values[0]
        gene.append(get_gene(disease))
        
        index = random.randint(0, len(variants_df)-1)
        conding_name = variants_df["coding_name"][index]
        protein_name = variants_df["protein_name"][index]
        if isinstance(protein_name, float):
            mutation1.append(f'{conding_name}')
        else:
            mutation1.append(f'{conding_name};{protein_name.replace("p.","p(")})')
        
    
        index = random.randint(0, len(variants_df)-1)
        conding_name = variants_df["coding_name"][index]
        protein_name = variants_df["protein_name"][index]
        if isinstance(protein_name, float):
            mutation2.append(f'{conding_name}')
        else:
            mutation2.append(f'{conding_name};{protein_name.replace("p.","p(")})')
        
    data = {'Patient ID': patient_id, 
            "Inheritance pattern": inheritance,
            "Gene": gene,
            "Mutation 1": mutation1,
            "Mutation 2": mutation2}
    
    df = pd.DataFrame(data=data)
    
    return df
        

def generate_dynamic_dataset(baseline_df, num_evolution_records):
    id_list = []
    age = []
    reva = []
    leva = []
    reref = []
    leref = []
    rodre = []
    rodle = []
    mixrea = []
    mixlea = []
    mixreb = []
    mixleb = []
    conereamp = []
    coneleamp = []
    conereimp = []
    coneleimp = []
    pvfre = []
    pvfle = []
    cvfre = []
    cvfle = []
    rcfr_iii = []
    rcfl_iii = []
    rcfr_v = []
    rcfl_v = []
    
    for id in baseline_df["Patient ID"]:
        age_onset = baseline_df.loc[baseline_df['Patient ID'] == id]['Onset'].values[0]
        for i in range(num_evolution_records):
            id_list.append(id)
            
            age_onset = age_onset + 1
            age.append(age_onset)
            
            visual_acuity = random.randint(a=0, b=100)
            reva.append(visual_acuity/100)
            
            visual_acuity = random.randint(a=0, b=100)
            leva.append(visual_acuity/100)
            
            myop_hiper =random.choice(["-", "+"])
            diop = random.randint(1, 8)
            med = random.choice([1,0])
            if med:
                reref.append(f'{myop_hiper}{diop}.{5}')
            else:
                reref.append(f'{myop_hiper}{diop}')
                             
            myop_hiper =random.choice(["-", "+"])
            diop = random.randint(1, 8)
            med = random.choice([1,0])
            
            if med:
                leref.append(f'{myop_hiper}{diop}.{5}')
            else:
                leref.append(f'{myop_hiper}{diop}')
            
            rodre.append(random.choice([0, "NA", random.randint(a=100, b=300)]))
            rodle.append(random.choice([0, "NA", random.randint(a=100, b=300)]))
            
            mixrea.append(random.choice([0, "NA", random.randint(a=150, b=250)]))
            mixlea.append(random.choice([0, "NA", random.randint(a=150, b=250)]))
            
            mixreb.append(random.choice([0, "NA", random.randint(a=300, b=600)]))
            mixleb.append(random.choice([0, "NA", random.randint(a=300, b=600)]))
            
            conereamp.append(random.choice([0, "NA", random.randint(a=20, b=60)]))
            coneleamp.append(random.choice([0, "NA", random.randint(a=20, b=60)]))
            
            conereimp.append(random.choice([0, "NA", random.randint(a=25, b=35)]))
            coneleimp.append(random.choice([0, "NA", random.randint(a=25, b=35)]))
            
            pvfre.append(random.choice(["yes", "no"]))
            pvfle.append(random.choice(["yes", "no"]))
            cvfre.append(random.choice(["yes", "no"]))
            cvfle.append(random.choice(["yes", "no"]))
            
            rcfr_iii.append(random.randint(a=25, b=35))
            rcfl_iii.append(random.randint(a=25, b=35))
            rcfr_v.append(random.randint(a=20, b=30))
            rcfl_v.append(random.randint(a=20, b=30))
                
    data = {"Patient ID": id_list, "Age": age, "REVA": reva, "LEVA": leva, "REref": reref, "LEref": leref,
            "RodRE": rodre, "RodLE": rodle, "MixREA": mixrea, "MixLEA": mixlea, "MixREB": mixreb, "MixLEB": mixleb,
            "ConeREamp": conereamp, "ConeLEamp": coneleamp, "ConeREimp": conereimp, "ConeLEimp": coneleimp,
            "PVFRE": pvfre, "PVFLE": pvfle, "CVFRE": cvfre, "CVFLE": cvfle, "RCFR_III": rcfr_iii, "RCFL_III": rcfl_iii,
            "RCFR_V": rcfr_v, "RCFL_V": rcfl_v}
    
    df = pd.DataFrame(data=data)

    return df


def generate_imaging_dataset(baseline_df):
    
    id_list = baseline_df["Patient ID"]
    fundus = []
    wfre = []
    wfle = []
    wffafre = []
    wffafle = []
    cmere = []
    cmele = []
    octre = []
    octle = []
    bc = []
    
    for id in id_list:
        fundus.append(random.choice(FUNDUS_DESCRIPTIONS))
        wfre.append(random.choice(WIDE_FIELD_COLOR))
        wfle.append(random.choice(WIDE_FIELD_COLOR))
        wffafre.append(random.choice(WIDE_FIELD_AUTOFLUORESCENCE))
        wffafle.append(random.choice(WIDE_FIELD_AUTOFLUORESCENCE))
        cmere.append(random.choice(["yes","no"]))
        cmele.append(random.choice(["yes","no"]))
        octre.append(random.choice(CENTRAL_OPTICAL_COHERENCE))
        octle.append(random.choice(CENTRAL_OPTICAL_COHERENCE))
        bc.append(random.choice(["yes","no"]))
    
    data = {"Patient ID": id_list, "Fundus": fundus, "WFRE": wfre, "WFLE": wfle, 
            "WFFAFRE": wffafre, "WFFAFLE": wffafle, "CMERE": cmere, "CMELE": cmele,
            "OCTRE": octre, "OCTLE": octle, "BC": bc}
    
    df = pd.DataFrame(data=data)

    return df


def main():
    if len(sys.argv) < 5:
        print("Ussage: py BUZZI.py <num_rows> <output_file_name_1> <output_file_name_2> <output_file_name_3> <output_file_name_4>")
    else:
        num_rows = int(sys.argv[1])
        output_file_1 = sys.argv[2]
        output_file_2 = sys.argv[3]
        output_file_3 = sys.argv[4]
        output_file_4 = sys.argv[5]
        
        # Create the baseline clinical table
        fieldset = generate_baseline_clinical_dataset()
        hf = headfake.HeadFake.from_python({"fieldset":fieldset})
        data_1 = hf.generate(num_rows=num_rows)

        # Create CSV output
        data_1.to_csv(output_file_1, index=False)
        print(f"Baseline results written to {output_file_1}")
        
        # Create the genetic table
        data_2 = generate_genetic_dataset(data_1)
        
        # Create the CSV output
        data_2.to_csv(output_file_2, index=False)
        print(f"Genetic results written to {output_file_2}")
        
        # Create the dynamic table
        data_3 = generate_dynamic_dataset(data_1, 4)
        
        # Create the CSV output
        data_3.to_csv(output_file_3, index=False)
        print(f"Dynamic clinical results written to {output_file_3}")
        
        # Create the imaging data table
        data_4 = generate_imaging_dataset(data_1)
        
        # Create the CSV output
        data_4.to_csv(output_file_4, index=False)
        print(f"Imaging results written to {output_file_4}")

if __name__ == "__main__":
    main()