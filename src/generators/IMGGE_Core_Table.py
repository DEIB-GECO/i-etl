import headfake
import headfake.field
import headfake.transformer
import scipy
import operator
import sys
import datetime
import pandas as pd
import re
import random

import scipy.stats

def age_months(dob, now):
        first_month = datetime.datetime.strptime(dob, "%d.%m.%Y").month
        second_month = now.month
        if second_month >= first_month:
            return second_month - first_month
        else:
            return 0

# TODO: Truncate result to only 2 decimals
def age_summary(age_number_y, age_number_m):
        return (age_number_y + age_number_m)/12

def age_to_string(years, months):
    if years == 0:
        return f"{months}m"
    elif months == 0:
        return f"{years}y"
    else:
        return f"{years}y, {months}m"

def transform_omimID(omimID, dummy_value):
    if omimID != None:
        return omimID.replace("# ", "OMIM:")
    else:
        return omimID

# TODO: There can be more than one OMIM ID
# TODO: Remove undesired HP values such as Recessive inheritance
# TODO: Check how to extrac read_csv from this function
def get_hpo_descriptive(omim_id, symptoms_max_number):
    if omim_id != None:
        df = pd.read_csv('DATA/genes_to_phenotype.txt', sep="\t")
        x = df.loc[df['disease_id'] == omim_id]

        result =[]

        for i, row in x.iterrows():
            hpo = row["hpo_id"]
            hpo_name = row["hpo_name"]
            result.append(f"{hpo_name} {hpo}")

            if len(result) > symptoms_max_number:
                result = result[1:symptoms_max_number]
            
        return ','.join(result)
    else:
        return None

def get_hpo_identifiers(symptoms, dummy_value):
    if symptoms != None:
        symptoms_list = symptoms.split(",")
        result = []
        for symptom in symptoms_list:
            m = re.search('HP:[0-9]+', symptom)
            if m:
                result.append(m.group(0))
        return ", ".join(result)
    return None

# TODO: Refine hospital names
def generate_hospital_name_by_country(country, dummy_value):

    df = pd.read_csv('DATA/countries.csv', sep=",")
    x = df.loc[df['Name'] == country]
    
    if len(x) is None:
        return "Country not supported."
    
    adjectives = []
    locations = []
    hospital_terms = []

    for i, row in x.iterrows():
        adjectives = row["Adjectives"].split(",")
        locations = row["Locations"].split(",")
        hospital_terms = row["hospital_terms"].split(",")
    
    adjective = random.choice(adjectives)
    location = random.choice(locations)
    hospital_term = random.choice(hospital_terms)

    hospital_name = f"{adjective} {location} {hospital_term}"
    return hospital_name

def generate_karyotype_result(sex, dummy_value):
    if sex == "1": # Male
        random.choices(["46, XY", "47,XY,+21", "46,XY,del(1)(p36)", "46,XY,del(18)(q21.3q33)"])
    elif sex == "2": # Female
        random.choices(["46, XX", "47,XX,+21", "46,XX,del(1)(p36)", "46,XX,del(18)(q21.3q33)"])
    else:
        random.choices(["46, XY", "46, XX"])

def generate_fields():
    # PATIENT ID
    idGenerator = headfake.field.IncrementIdGenerator(min_value=1, length=3)
    patientID = headfake.field.IdField(name="record_id", prefix="RFZO ", generator=idGenerator)

    # DIAGNOSIS NAME
    diagnosisName = headfake.field.MapFileField(mapping_file="DATA/IMGGE_Diseases.csv", key_field="disease_name", name="diagnosis")

    # DIAGNOSIS OMIM IDENTIFIER
    diagnosis_omimID = headfake.field.LookupMapFileField(lookup_value_field="OMIM_Phenotype", map_file_field="diagnosis", name="omim_pheno")

    # GENE OMIM IDENTIFIER
    gene_omimID = headfake.field.LookupMapFileField(lookup_value_field="OMIM_Gene", map_file_field="diagnosis", name="omim_gene_locus")

    # DIAGNOSIS ICD-10 CODE
    icd10Code = headfake.field.LookupMapFileField(lookup_value_field="ICD_10", map_file_field="diagnosis", name="icd10_ref_diagnosis")

    # DATE OF BIRTH
    dateOfBirth = headfake.field.DateOfBirthField(name="dob", distribution=scipy.stats.uniform, mean=0, sd=18, min=0, max=0, date_format="%d.%m.%Y")

    # COUNTRY OF BIRTH
    countryOfBirth = headfake.field.MapFileField(mapping_file="DATA/countries.csv", key_field="Name", name="country_birth")

    # SEX PROBABILITIES
    sexProbabilites = {"1":0.4, "2":0.5, "99":0.1} # {MALE, FEMALE, INDETERMINATE SEX}
    sex = headfake.field.OptionValueField(probabilities=sexProbabilites, name="sex")

    # NUMBER OF YEARS WHEN TESTED
    dobFieldValue = headfake.field.LookupField(field="dob", hidden=True)
    currentAge = headfake.field.derived.AgeField(from_value=dobFieldValue, to_value=datetime.datetime.now(), from_format="%d.%m.%Y", to_format="%d.%m.%Y", hidden=True)
    testedYears = headfake.field.OperationField(name="age_number_y", operator=operator.floordiv, first_value=currentAge, second_value=2)

    # NUMBER OF MONTHS WHEN TESTED
    testedMonths = headfake.field.OperationField(name="age_number_m", operator=None, first_value=dobFieldValue, second_value=datetime.datetime.now(), operator_fn=age_months)

    # AGE WHEN TESTED IN YEARS
    testedsummary = headfake.field.OperationField(name="age_summary", operator=None, first_value=testedYears, second_value=testedMonths, operator_fn=age_summary)

    # AGE WHEN SENT FOR GENETIC TESTING
    ageWhenTested = headfake.field.OperationField(name="age_sent_for_genet", operator=None, first_value=testedYears, second_value=testedMonths, operator_fn=age_to_string)
    
    # THERE IS CONSANGUINITY
    consanguinityProbabilities = {"1":0.8, "2":0.1, "99":0.1} # {No, Yes, Not applicable}
    hasConsanguinity = headfake.field.OptionValueField(probabilities=consanguinityProbabilities, name="consangui_offsp")

    # CONSANGUINEOUS RELATIVES
    offspringProbabilities = {"First":0.8, "Second":0.2}
    condition = headfake.field.Condition(field="consangui_offsp", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=offspringProbabilities)
    consanguinityRelation = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value="no", name="consangui_offsp_rel")

    # HAS AFFECTED RELATIVES
    affectedRelativesProbabilities = {"1":0.6, "2":0.3, "99":0.1} # {No, Yes, Unknown}
    hasAffectedRelatives = headfake.field.OptionValueField(probabilities=affectedRelativesProbabilities, name="relatives")

    # AFFECTED RELATIVES
    relativesProbabilities = {"Mother":0.2, "Father":0.2, "Brother":0.1, "Sister":0.1, "Uncle":0.1, "Aunt":0.1, "Grandfather":0.1, "Grandmother":0.1 }
    condition = headfake.field.Condition(field="relatives", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=relativesProbabilities)
    affectedRelatives = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None, name="relative_rel")

    # HAS SPONTANEOUS ABORTIONS
    abortionProbabilities = {"1":0.6, "2":0.3, "99":0.1} # {No, Yes, Unknown}
    hasSpontaneousAbortions = headfake.field.OptionValueField(probabilities=abortionProbabilities, name="sp_abortion")

    # NUMBER OF SPONTANEOUS ABORTIONS
    numAbortionsProbabilities = {"1":0.5, "2":0.2, "3":0.2, "4":0.1}
    condition = headfake.field.Condition(field="sp_abortion", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=numAbortionsProbabilities)
    numberOfAbortions = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None, name="sp_abortion_numb")

    # IS IN VITRO FERTILIZATION
    ivProbabilities = {"1":0.7, "2":0.2, "99":0.1} # {No, Yes, Unknown}
    inVitroFertilization = headfake.field.OptionValueField(probabilities=ivProbabilities, name="ivf")
      
    # LIST OF SYMPTOMS WITH HPO IDENTIFIER
    omimID = headfake.field.OperationField(operator=None, first_value=diagnosis_omimID, second_value=None, operator_fn=transform_omimID, hidden=True)
    hpo_descriptive = headfake.field.OperationField(name="hpo_descriptive", operator=None, first_value=omimID, second_value=5, operator_fn=get_hpo_descriptive)

    # LIST OF SYMPTOMS HPO IDENTIFIERS
    hpo_data = headfake.field.OperationField(name="hpo_data", operator=None, first_value=hpo_descriptive, second_value=None, operator_fn=get_hpo_identifiers)

    # YEARS AT ONSET
    onsetYears = headfake.field.OperationField(name="age_onset_number_y", operator=operator.floordiv, first_value=currentAge, second_value=2)

    # MONTHS AT ONSET
    onsetMonths = headfake.field.OperationField(name="age_onset_number_m", operator=None, first_value=dobFieldValue, second_value=datetime.datetime.now(), operator_fn=age_months)

    # AGE AT ONSET IN YEARS
    onsetSummary = headfake.field.OperationField(name="age_onset_summary", operator=None, first_value=testedYears, second_value=testedMonths, operator_fn=age_summary)

    # AGE AT ONSET (DESCRIPTIVE)
    ageOnset = headfake.field.OperationField(name="age_onset", operator=None, first_value=onsetYears, second_value=onsetMonths, operator_fn=age_to_string)

    # HAS INTELECTUAL DISABILITY
    idProbabilities = {"1":0.1, "2":0.8, "99":0.1} # {No, Yes, Not applicable}
    hasID = headfake.field.OptionValueField(probabilities=idProbabilities, name="intel_dis")

    # IQ TEST RESULT
    condition = headfake.field.Condition(field="intel_dis", operator=operator.eq, value="2")
    transformer = headfake.transformer.ConvertToNumber(as_integer=True)
    true_value = headfake.field.NumberField(distribution=scipy.stats.norm, mean=50, sd=30, min=10, max=70, transformers=[transformer])
    testResult = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value="no", name="iq_test")

    # IQ TEST DESCRIPTIVE RESULT
    condition1 = headfake.field.Condition(field="iq_test", operator=operator.lt, value=20)
    true_value1 = "Profound Intellectual Disability"
    
    condition2 = headfake.field.Condition(field="iq_test", operator=operator.lt, value=34)
    true_value2 = "Severe Intellectual Disability"
    
    condition3 = headfake.field.Condition(field="iq_test", operator=operator.lt, value=49)
    true_value3 = "Moderate Intellectual Disability"
    
    condition4 = headfake.field.Condition(field="iq_test", operator=operator.lt, value=69)
    true_value4 = "Mild Intellectual Disability"
    
    false_value5 = "no"
    false_value4 = headfake.field.IfElseField(condition=condition4, true_value=true_value4, false_value=false_value5)
    false_value3 = headfake.field.IfElseField(condition=condition3, true_value=true_value3, false_value=false_value4)
    false_value2 = headfake.field.IfElseField(condition=condition3, true_value=true_value3, false_value=false_value3)
    false_value1 = headfake.field.IfElseField(condition=condition2, true_value=true_value2, false_value=false_value2)
    
    condition = headfake.field.Condition(field="itel_dis", operator=operator.eq, value="2")
    true_value = headfake.field.IfElseField(condition=condition1, true_value=true_value1, false_value=false_value1)
    IQTestDescription = headfake.field.IfElseField(name="iq_test_desc", condition=condition, true_value=true_value, false_value="no")

    # HAS OTHER ID TEST
    condition=headfake.field.Condition(field="intel_dis", operator=operator.eq, value="2")
    otherProbabilities = {"1":0.1, "2":0.8, "99":0.1} # {No, Yes, Not applicable}
    true_value = headfake.field.OptionValueField(probabilities=otherProbabilities)
    otherIdTest = headfake.field.IfElseField(name="oth_iq_test_yesno",condition=condition, true_value=true_value, false_value="1")

    # OTHER ID TEST NAME
    condition=headfake.field.Condition(field="oth_iq_test_yesno", operator=operator.eq, value="2")
    true_value=headfake.field.MapFileField(mapping_file="DATA/intelligence_tests.csv", key_field="Name")
    otherIdTestName = headfake.field.IfElseField(name="oth_iq_test_name", condition=condition, true_value=true_value, false_value=None)

    # OTHER ID TEST RESULT
    condition = headfake.field.Condition(field="oth_iq_test_yesno", operator=operator.eq, value="2")
    otherIdResultProbabilities = {"Average":0.4,"Below average":0.6}
    true_value = headfake.field.OptionValueField(probabilities=otherIdResultProbabilities)
    otherIDTestResult = headfake.field.IfElseField(name="oth_iq_test_result", condition=condition, true_value=true_value, false_value="no")

    # HAS METABOLIC DISEASE
    metabolicProbabilities = {"1":0.1, "2":0.8, "99":0.1} # {No, Yes, Unknown}
    hasMetabolicDisease = headfake.field.OptionValueField(name="metab_disease", probabilities=metabolicProbabilities)

    # THERE ARE METABOLIC TEST MEASURES
    condition = headfake.field.Condition(field="metab_disease", operator=operator.eq, value="2")
    metabolicMeasuresProbabilities = {"1":0.1, "2":0.8, "99":0.1} # {No, Yes, Unknown}
    true_value = headfake.field.OptionValueField(probabilities=metabolicMeasuresProbabilities)
    hasMetabolicMeasures = headfake.field.IfElseField(name="metab_disease_measure", condition=condition, true_value=true_value, false_value="1")

    # HAS COMPUTED TOMOGRAPHY
    ctProbabilities = {"1":0.3, "2":0.6, "99":0.1} # {No, Yes, Unknown}
    hasCT = headfake.field.OptionValueField(name="ct_scan", probabilities=ctProbabilities)

    # HAS MAGNETIC RESONANCE
    mrProbabilities = {"1":0.3, "2":0.6, "99":0.1} # {No, Yes, Unknown}
    hasMR = headfake.field.OptionValueField(name="mr_scan", probabilities=mrProbabilities)

    # HAS EEG
    eegProbabilities = {"1":0.3, "2":0.6, "99":0.1} # {No, Yes, Unknown}
    hasEEG = headfake.field.OptionValueField(name="eeg", probabilities=eegProbabilities)
    
    # CLINICAL CENTER OF REFERRAL
    country_value = headfake.field.LookupField(field="country_birth")
    clinic = headfake.field.OperationField(operator=None, first_value=country_value, second_value=None, operator_fn=generate_hospital_name_by_country)

    # HAS PREVIOUS KARYOTYPE
    karyotypeProbabilities = {"1":0.5, "2":0.4, "99":0.1} # {No, Yes, Not applicable}
    hasPreviouskaryotype = headfake.field.OptionValueField(name="karyotype", probabilities=karyotypeProbabilities)
    
    # PREVIOUS KARYOTYPE RESULT
    condition = headfake.field.Condition(field="karyotype", operator=operator.eq, value="2")
    true_value = headfake.field.OperationField(operator=None, first_value=sex, second_value=None, operator_fn=generate_karyotype_result)
    karyotype_result = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value="no")
    
    # HAS PREVIOUS MICROARRAY
    microarrayProbabilities = {"1":0.5, "2":0.4, "99":0.1} # {No, Yes, Not applicable}
    hasPreviousMicroarray = headfake.field.OptionValueField(name="microarray", probabilities=microarrayProbabilities)
    
    # PREVIOUS MICROARRAY RESULT
    microarrayResultProbabilities = {"positive":0.5, "negative":0.4, "inconclusive":0.1}
    condition = headfake.field.Condition(field="microarray", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=microarrayResultProbabilities)
    microarrayResult = headfake.field.IfElseField(name="microarray_result", condition=condition, true_value=true_value, false_value=None)
    
    # TODO:
    microarray_sig = headfake.field.ConstantField(name="microarray_sig", value=None)
    
    # TODO:
    microarray_type = headfake.field.ConstantField(name="microarray_type", value=None)
    
    # TODO:
    microarray_reg_no = headfake.field.ConstantField(name="microarray_reg_no", value=None)
    
    # TODO:
    microarray_lenght = headfake.field.ConstantField(name="microarray_length", value=None)
    
    # TODO:
    microarray_details = headfake.field.ConstantField(name="microarray_details", value=None)
    
    # HAS OTHER PREVIOUS GENETIC TEST
    previousProbabilities = {"1":0.5, "2":0.4, "99":0.1} # {No, Yes, Not applicable}
    hasPreviousOther = headfake.field.OptionValueField(name="other_gen_test", probabilities=previousProbabilities)
    
    # OTHER GENETIC TEST TYPE
    otherProbabilities = {"MLPA":0.3, "FraX":0.3, "DNA methylation test":0.2, "other":0.2}
    condition = headfake.field.Condition(field="other_gen_test", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=otherProbabilities)
    otherType = headfake.field.IfElseField(name="other_gen_test_type", condition=condition, true_value=true_value, false_value=None)
    
    # OTHER GENETIC TEST RESULT
    otherResultProbabilities = {"positive":0.5, "negative":0.4, "inconclusive":0.1}
    condition = headfake.field.Condition(field="other_gen_test", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=otherResultProbabilities)
    otherResult = headfake.field.IfElseField(name="other_gen_test_typeresult", condition=condition, true_value=true_value, false_value=None)
    
    # GENETIC TEST RESULT
    genProbabilities = {"positive":0.7, "negative":0.2, "inconclusive":0.1}
    ngsTestResult = headfake.field.OptionValueField(name="gen_result", probabilities=genProbabilities)
    
    # GENETIC TEST APPROACH
    approachProbabilities = {"CES":0.3, "WES":0.4, "WGS":0.3}
    ngsTestApproach = headfake.field.OptionValueField(name="gen_result", probabilities=approachProbabilities)
    
    # SEQUENCING PLATFORM
    platformProbabilities = {"MiSeq, Illumina":0.2, "NextSeq550, Illumina":0.3, "NextSeq 2000, Illumina":0.2, "DNBSEQ G-400, MGI":0.3}
    ngsTestPlatform = headfake.field.OptionValueField(name="gen_ngsplatform", probabilities=platformProbabilities)
    
    # TODO:
    ngsTestOtherPlatform = headfake.field.ConstantField(name="oth_gen_ngsplatform", value=None)
    
    # REFERENCE GENOME
    refProbabilities = {"GRCh 37 (hg19)":0.5, "GRCh 38":0.5}
    refGenome = headfake.field.OptionValueField(name="gen_ref", probabilities=refProbabilities)
    
    # GENE 1 NAME
    gen_gen1 = headfake.field.ConstantField(name="gen_gen1", value=None)
    
    # TODO:
    gen_chromosome1 = headfake.field.ConstantField(name="gen_chromosome1", value=None)
    
    # NUMBER OF VARIANTS FOUND IN GENE 1
    novarProbabilities = {1:0.5, 2:0.5}
    gen_novar1 = headfake.field.OptionValueField(name="gen_novar1", probabilities=novarProbabilities)
    
    # VARIANT INHERITANCE IN GENE 1
    inheritanceProbabilities = {"2":0.3, "1":0.3, "3":0.2, "4":0.1, "5":0.1} # {"Autosomal recessive inheritance", "Autosomal dominant inheritance", "X-linked inheritance, dominant", "X-linked inheritance, recessive", "Mitochondrial mutation"}
    variantInheritance1 = headfake.field.OptionValueField(name="gen_inheritance1", probabilities=inheritanceProbabilities)
    
    # VARIANT 1 ZYGOSITY
    zygosityProbabilities={"heterozygosis":0.3, "het":0.3, "homozygosis":0.2, "hom":0.1, "hemizygous":0.1}
    zygosity1 = headfake.field.OptionValueField(name="gen_zigosity1", probabilities=zygosityProbabilities)
    
    # TRANSCRIPT IDENTIFIER OF GENE 1
    # TODO: Change to random for the generation of random patches
    generator = headfake.field.RandomReuseIdGenerator(length=6, min_value=1645)
    gen_transcript1 = headfake.field.IdField(name="gen_transcript1", prefix='NM_', suffix=".4", generator=generator)
    
    # TODO:
    gen_varnamecdna1_1 = headfake.field.ConstantField(name="gen_varnamecdna1_1", value=None)
    
    # TODO:
    gen_varnameprot1_1 = headfake.field.ConstantField(name="gen_varnameprot1_1", value=None)
    
    # TODO:
    segregationProbabilities = {"No":0.2, "1":0.2, "2":0.2, "3":0.2, "4":0.2}
    segregation1 = headfake.field.OptionValueField(name="segregarion_result1", probabilities=segregationProbabilities)
    
    # TODO:
    gen_varnamecdna1_2 = headfake.field.ConstantField(name="gen_varname_cdna1_2", value=None)
    
    # TODO:
    gen_varnameprot1_2 = headfake.field.ConstantField(name="gen_varnameprot1_2", value=None)
    
    # TODO:
    segregation_result1_2 = headfake.field.ConstantField(name="segregation_result1_2", value=None)
    
    # TODO:
    secgen2 = headfake.field.ConstantField(name="secgen2", value=None)
    
    # TODO:
    secgen_gene2 = headfake.field.ConstantField(name="secgen_gene2", value=None)
    
    # TODO:
    secgen_chromosome2 = headfake.field.ConstantField(name="secgen_chromosome2", value=None)
    
    # TODO:
    secgen_novar2 = headfake.field.ConstantField(name="secgen_novar2", value=None)
    
    # TODO:
    secgen_inheritance2 = headfake.field.ConstantField(name="secgen_inheritance2", value=None)
    
    # TODO:
    secgen_zigosity2 = headfake.field.ConstantField(name="secgen_zigosity2", value=None)
    
    # TRANSCRIPT IDENTIFIER OF GENE 2
    # TODO: Change to random for the generation of random patches
    generator = headfake.field.RandomReuseIdGenerator(length=6, min_value=1645)
    secgen_transcript2 = headfake.field.IdField(name="secgen_transcript2", prefix='NM_', suffix=".4", generator=generator)
    
    # TODO:
    secgen_varnamecdna2_1 = headfake.field.ConstantField(name="secgen_varnamecdna2_1", value=None)
    
    # TODO:
    secgen_varnameprot2_1 = headfake.field.ConstantField(name="secgen_varnameprot2_1", value=None)
    
    # TODO:
    secgen_segregation2_1 = headfake.field.ConstantField(name="secgen_segregation2_1", value=None)
    
    # TODO:
    secgen_varnamecdna2_2 = headfake.field.ConstantField(name="secgen_varnamecdna2_2", value=None)
    
    # TODO:
    secgen_varnameprot2_2 = headfake.field.ConstantField(name="secgen_varnameprot2_2", value=None)
    
    # TODO:
    secgen_segregation2_2 = headfake.field.ConstantField(name="secgen_segregation2_2", value=None)
    
    # TODO:
    loc_vcf = headfake.field.ConstantField(name="loc_vcf", value=None)
    
    # TODO:
    oth_loc_vcf = headfake.field.ConstantField(name="other_loc_vcf", value=None)
    
    # TODO:
    oth_loc_vcf_ext = headfake.field.ConstantField(name="other_loc_vcf_ext", value=None)
    
    # TODO:
    comment = headfake.field.ConstantField(name="comment", value=None)


    # Create fieldset
    fs = headfake.Fieldset(fields=[patientID, diagnosisName, diagnosis_omimID, gene_omimID, icd10Code, dateOfBirth, countryOfBirth, sex,
                            ageWhenTested, testedYears, testedMonths, testedsummary, hasConsanguinity, consanguinityRelation, 
                            hasAffectedRelatives, affectedRelatives, hasSpontaneousAbortions, numberOfAbortions, inVitroFertilization,
                            hpo_descriptive, hpo_data, ageOnset, onsetYears, onsetMonths, onsetSummary, hasID, 
                            testResult,IQTestDescription,otherIdTest, otherIdTestName, otherIDTestResult, hasMetabolicDisease, 
                            hasMetabolicMeasures, hasCT, hasMR, hasEEG, clinic, hasPreviouskaryotype, karyotype_result, hasPreviousMicroarray, 
                            microarrayResult, microarray_sig, microarray_type, microarray_reg_no, microarray_lenght, microarray_details, hasPreviousOther,
                            otherType, otherResult, ngsTestResult, ngsTestApproach, ngsTestPlatform, ngsTestOtherPlatform, refGenome, gen_gen1, 
                            gen_chromosome1, gen_novar1, variantInheritance1, zygosity1, gen_transcript1, gen_varnamecdna1_1, gen_varnameprot1_1, 
                            segregation1, gen_varnamecdna1_2, gen_varnameprot1_2, segregation_result1_2, secgen2, secgen_gene2, secgen_chromosome2, 
                            secgen_novar2, secgen_inheritance2, secgen_zigosity2, secgen_transcript2, secgen_varnamecdna2_1, secgen_varnameprot2_1,
                            secgen_segregation2_1, secgen_varnamecdna2_2, secgen_varnameprot2_2, secgen_segregation2_2, loc_vcf, oth_loc_vcf, oth_loc_vcf_ext, 
                            comment])
    
    return fs

def main():
    if len(sys.argv) < 2:
        print("Ussage: py BETTER_IMGGE_Core_Table.py <num_rows> <output_file_name>")
    else:
        num_rows = int(sys.argv[1])
        output_file = sys.argv[2]
    
        fieldset = generate_fields()
        hf = headfake.HeadFake.from_python({"fieldset":fieldset})

        # Create the dataset
        data = hf.generate(num_rows=num_rows)

        # Create CSV output
        data.to_csv(output_file, index=False)
    
        print(f"Results written to {output_file}")

if __name__ == "__main__":
    main()