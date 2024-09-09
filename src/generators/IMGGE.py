import headfake
import headfake.field
import headfake.transformer
import scipy
import operator
import sys
import datetime
import pandas as pd

from CM_MODULES.Diagnosis import Diagnosis
from CM_MODULES.Patient import Patient, Age
from CM_MODULES.GeneticTest import Karyotype, Microarray
from CM_MODULES.Gene import Gene
from CM_MODULES.Variant import Variant

# READ COMPLEMENTARY DATASETS
df_genes_to_phenotype = pd.read_csv('DATA/genes_to_phenotype.txt', sep="\t")
df_countries = pd.read_csv('DATA/countries.csv', sep=",")
df_diseases = pd.read_csv('DATA/IMGGE_Diseases.csv', sep=",")


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
    testedMonths = headfake.field.OperationField(name="age_number_m", operator=None, first_value=dobFieldValue, second_value=datetime.datetime.now(), operator_fn=Age.get_age_months)

    # AGE WHEN TESTED IN YEARS
    testedsummary = headfake.field.OperationField(name="age_summary", operator=None, first_value=testedYears, second_value=testedMonths, operator_fn=Age.get_age_summary)

    # AGE WHEN SENT FOR GENETIC TESTING
    ageWhenTested = headfake.field.OperationField(name="age_sent_for_genet", operator=None, first_value=testedYears, second_value=testedMonths, operator_fn=Age.age_to_string)
    
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
    omimID = headfake.field.OperationField(operator=None, first_value=diagnosis_omimID, second_value=None, operator_fn=Diagnosis.transform_omimID, hidden=True)
    hpo_descriptive = headfake.field.OperationField(name="hpo_descriptive", operator=None, first_value=omimID, second_value=5, operator_fn=Diagnosis.get_symptoms)

    # LIST OF SYMPTOMS HPO IDENTIFIERS
    hpo_data = headfake.field.OperationField(name="hpo_data", operator=None, first_value=hpo_descriptive, second_value=None, operator_fn=Diagnosis.get_hpo_identifiers)

    # YEARS AT ONSET
    # TODO: Create different than testing
    onsetYears = headfake.field.OperationField(name="age_onset_number_y", operator=operator.floordiv, first_value=currentAge, second_value=2)

    # MONTHS AT ONSET
    # TODO: Create different than testing
    onsetMonths = headfake.field.OperationField(name="age_onset_number_m", operator=None, first_value=dobFieldValue, second_value=datetime.datetime.now(), operator_fn=Age.get_age_months)

    # AGE AT ONSET IN YEARS
    onsetSummary = headfake.field.OperationField(name="age_onset_summary", operator=None, first_value=testedYears, second_value=testedMonths, operator_fn=Age.get_age_summary)

    # AGE AT ONSET (DESCRIPTIVE)
    ageOnset = headfake.field.OperationField(name="age_onset", operator=None, first_value=onsetYears, second_value=onsetMonths, operator_fn=Age.age_to_string)

    # HAS INTELECTUAL DISABILITY
    idProbabilities = {"1":0.1, "2":0.8, "99":0.1} # {No, Yes, Not applicable}
    hasID = headfake.field.OptionValueField(probabilities=idProbabilities, name="intel_dis")

    # IQ TEST RESULT
    condition = headfake.field.Condition(field="intel_dis", operator=operator.eq, value="2")
    transformer = headfake.transformer.ConvertToNumber(as_integer=True)
    true_value = headfake.field.NumberField(distribution=scipy.stats.norm, mean=50, sd=30, min=10, max=70, transformers=[transformer])
    testResult = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value="no", name="iq_test")

    # IQ TEST DESCRIPTIVE RESULT
    # TODO: Simplify
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
    clinic = headfake.field.OperationField(name="clinic", operator=None, first_value=country_value, second_value=None, operator_fn=Patient.generate_hospital_name_by_country)

    # HAS PREVIOUS KARYOTYPE
    karyotypeProbabilities = {"1":0.5, "2":0.4, "99":0.1} # {No, Yes, Not applicable}
    hasPreviouskaryotype = headfake.field.OptionValueField(name="karyotype", probabilities=karyotypeProbabilities)
    
    # PREVIOUS KARYOTYPE RESULT
    condition = headfake.field.Condition(field="karyotype", operator=operator.eq, value="2")
    true_value = headfake.field.OperationField(operator=None, first_value=sex, second_value=None, operator_fn=Karyotype.generate_karyotype_result)
    karyotype_result = headfake.field.IfElseField(name="karyotype_result", condition=condition, true_value=true_value, false_value="no")
    
    # HAS PREVIOUS MICROARRAY
    microarrayProbabilities = {"1":0.5, "2":0.4, "99":0.1} # {No, Yes, Not applicable}
    hasPreviousMicroarray = headfake.field.OptionValueField(name="microarray", probabilities=microarrayProbabilities)
    
    # PREVIOUS MICROARRAY RESULT
    microarrayResultProbabilities = {"positive":0.5, "negative":0.4, "inconclusive":0.1}
    condition = headfake.field.Condition(field="microarray", operator=operator.eq, value="2")
    true_value = headfake.field.OptionValueField(probabilities=microarrayResultProbabilities)
    microarrayResult = headfake.field.IfElseField(name="microarray_result", condition=condition, true_value=true_value, false_value=None)
    
    # MICROARRAY RESULT SIGNIFICANCE
    true_value = headfake.field.OperationField(operator=None, first_value=microarrayResult, second_value=None, operator_fn=Microarray.get_microarray_significance)
    microarray_sig = headfake.field.IfElseField(name="microarray_sig", condition=condition, true_value=true_value, false_value=None)
    
    # MICROARRAY RESULT TYPE
    microarrayTypeProbabilities = {"1":0.4, "2":0.3, "3":0.3} # {"deletion","microdeletion","duplication"}
    microarray_type = headfake.field.OptionValueField(name="microarray_type", probabilities=microarrayTypeProbabilities)
    
    # MICROARRAY RESULT REGION
    true_value = headfake.field.OperationField(operator=None, first_value=None, second_value=None, operator_fn=Microarray.generate_cytogenetic_location)
    microarray_reg_no = headfake.field.IfElseField(name="microarray_reg_no", condition=condition, true_value=true_value, false_value=None)
    
    # MICROARRAY RESULT LENGTH
    true_value = headfake.field.OperationField(operator=None, first_value=None, second_value=None, operator_fn=Microarray.generate_microarray_length)
    microarray_length = headfake.field.IfElseField(name="microarray_length", condition=condition, true_value=true_value, false_value=None)
    
    # MICROARRAY RESULT DETAILS
    true_value = headfake.field.OperationField(operator=None, first_value=microarray_reg_no, second_value=microarray_type, operator_fn=Microarray.generate_microarray_details)
    microarray_details = headfake.field.IfElseField(name="microarray_details", condition=condition, true_value=true_value, false_value=None)
    
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
    platformProbabilities = {"MiSeq, Illumina":0.2, "NextSeq550, Illumina":0.2, "NextSeq 2000, Illumina":0.2, "DNBSEQ G-400, MGI":0.2, None:0.2}
    ngsTestPlatform = headfake.field.OptionValueField(name="gen_ngsplatform", probabilities=platformProbabilities)
    
    # OTHER SEQUENCING PLATFORM
    condition = headfake.field.Condition(field="gen_ngsplatform", operator=operator.eq, value=None)
    otherplatformProbabilities = {"Ion S5, Thermo Fisher Scientific":0.4, "Sequel II, Pacific Biosciences":0.4, "GridION, Oxford Nanopore Technologies":0.1, "SOLiD 5500xl, SOLiD":0.1}
    true_value = headfake.field.OptionValueField(probabilities=otherplatformProbabilities)
    ngsTestOtherPlatform = headfake.field.IfElseField(name="oth_gen_ngsplatform", condition=condition, true_value=true_value, false_value=None)
    
    # REFERENCE GENOME
    refProbabilities = {"GRCh 37 (hg19)":0.5, "GRCh 38":0.5}
    refGenome = headfake.field.OptionValueField(name="gen_ref", probabilities=refProbabilities)
    
    # GENE 1 NAME
    # TODO: THERE CAN BE MORE THAN ONE GENE
    gen_gen1 = headfake.field.LookupMapFileField(name="gen_gen1", lookup_value_field="Gene", map_file_field="diagnosis")
    
    # CHROMOSOME NAME
    gen_chromosome1 = headfake.field.OperationField(name="gen_chromosome1", operator=None, first_value=None, second_value=None, operator_fn=Gene.generate_chromosome)
    
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
    
    # CODING VARIANT 1 NAME
    # TODO: Create a HGVS name generator
    gen_varnamecdna1_1 = headfake.field.MapFileField(mapping_file="DATA/variants.csv", key_field="coding_name", name="gen_varnamecdna1_1")
    
    # PROTEIN VARIANT 1 NAME
    # TODO: Create a HGVS name generator
    gen_varnameprot1_1 = headfake.field.LookupMapFileField(name="gen_varnameprot1_1", lookup_value_field="protein_name", map_file_field="gen_varnamecdna1_1")
    
    # VARIANT 1 SEGREGATION
    segregationProbabilities = {"No":0.2, "1":0.2, "2":0.2, "3":0.2, "4":0.2} # {"No","Not applicable","De novo","Maternal","Paternal"}
    segregation1 = headfake.field.OptionValueField(name="segregation_result1_1", probabilities=segregationProbabilities)
    
    # CODING VARIANT 2 NAME
    # TODO: Create a HGVS name generator
    condition = headfake.field.Condition(field="gen_novar1", operator=operator.eq, value=2)
    true_value = headfake.field.MapFileField(mapping_file="DATA/variants.csv", key_field="coding_name", name="gen_varnamecdna1_2")
    gen_varnamecdna1_2 = headfake.field.IfElseField(name="gen_varnamecdna1_2", condition=condition, true_value=true_value, false_value=None)
    
    # PROTEIN VARIANT 2 NAME
    # TODO: Create a HGVS name generator
    condition = headfake.field.Condition(field="gen_novar1", operator=operator.eq, value=2)
    true_value = headfake.field.OperationField(operator=None, first_value=gen_varnamecdna1_2, second_value=None, operator_fn=Variant.get_protein_name)
    gen_varnameprot1_2 = headfake.field.IfElseField(name="gen_varnameprot1_2", condition=condition, true_value=true_value, false_value=None)
    
    # VARIANT 2 SEGREGATION
    segregation_result1_2 = headfake.field.OptionValueField(name="segregarion_result1_2", probabilities=segregationProbabilities)
    
    # THERE ARE OTHER GENES NOT RELATED TO ID OR METABOLIC DISEASE AFFECTED
    otherAffectedGenes = {"Yes":0.2, "No":0.8} # {"Yes","No"}
    secgen2 = headfake.field.OptionValueField(probabilities=otherAffectedGenes, name="secgen2")
    
    # NAME OF GENE 2
    condition = headfake.field.Condition(field="secgen2", operator=operator.eq, value="Yes")
    true_value = headfake.field.MapFileField(mapping_file="DATA/variants.csv", key_field="other_gene")
    secgen_gene2 = headfake.field.IfElseField(name="secgen_gene2", condition=condition, true_value=true_value, false_value=None)
    
    # CHROMOSOME 2
    true_value = headfake.field.OperationField(operator=None, first_value=None, second_value=None, operator_fn=Gene.generate_chromosome)
    secgen_chromosome2 = headfake.field.IfElseField(name="secgen_chromosome2", condition=condition, true_value=true_value, false_value=None)
    
    # NUMBER OF VARIANTS FOUND IN GENE 2
    true_value = headfake.field.OptionValueField(probabilities=novarProbabilities)
    secgen_novar2 = headfake.field.IfElseField(name="secgen_novar2", condition=condition, true_value=true_value, false_value=None)
    
    # VARIANT INHERITANCE REGARDING GENE 2
    true_value = headfake.field.OptionValueField(probabilities=inheritanceProbabilities)
    secgen_inheritance2 = headfake.field.IfElseField(name="secgen_inheritance2", condition=condition, true_value=true_value, false_value=None)
    
    # VARIANT ZYGOSITY REGARDING GENE 2
    true_value = headfake.field.OptionValueField(probabilities=zygosityProbabilities)
    secgen_zigosity2 = headfake.field.IfElseField(name="secgen_zigosity2", condition=condition, true_value=true_value, false_value=None)
    
    # TRANSCRIPT IDENTIFIER OF GENE 2
    # TODO: Change to random for the generation of random patches
    generator = headfake.field.RandomReuseIdGenerator(length=6, min_value=1645)
    true_value = headfake.field.IdField(prefix='NM_', suffix=".4", generator=generator)
    secgen_transcript2 = headfake.field.IfElseField(name="secgen_transcript2", condition=condition, true_value=true_value, false_value=None)
    
    # CODING NAME OF VARIANT 1
    true_value = headfake.field.MapFileField(mapping_file="DATA/variants.csv", key_field="coding_name", name="secgen_varnamecdna2_1")
    secgen_varnamecdna2_1 = headfake.field.IfElseField(name="secgen_varnamecdna2_1", condition=condition, true_value=true_value, false_value=None)
    
    # PROTEIN NAME OF VARIANT 1
    true_value = headfake.field.OperationField(operator=None, first_value=secgen_varnamecdna2_1, second_value=None, operator_fn=Variant.get_protein_name)
    secgen_varnameprot2_1 = headfake.field.IfElseField(name="secgen_varnameprot2_1", condition=condition, true_value=true_value, false_value=None)
    
    # VARIANT 1 SEGREGATION 
    true_value = headfake.field.OptionValueField(probabilities=segregationProbabilities)
    secgen_segregation2_1 = headfake.field.IfElseField(name="secgen_segregation2_1", condition=condition, true_value=true_value, false_value=None)
    
    # CODING NAME OF VARIANT 2
    condition = headfake.field.Condition(field="secgen_novar2", operator=operator.eq, value=2)
    true_value = headfake.field.MapFileField(mapping_file="DATA/variants.csv", key_field="coding_name", name="secgen_varnamecdna2_1")
    true_value_2 = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None)
    secgen_varnamecdna2_2 = headfake.field.IfElseField(name="secgen_varnamecdna2_2", condition=condition, true_value=true_value_2, false_value=None)
    
    # PROTEIN NAME OF VARIANT 2
    condition = headfake.field.Condition(field="secgen_novar2", operator=operator.eq, value=2)
    true_value = headfake.field.OperationField(operator=None, first_value=secgen_varnamecdna2_2, second_value=None, operator_fn=Variant.get_protein_name)
    true_value_2 = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None)
    secgen_varnameprot2_2 = headfake.field.IfElseField(name="gen_varnameprot2_2", condition=condition, true_value=true_value_2, false_value=None)
    
    # VARIANT 2 SEGREGATION
    true_value = headfake.field.OptionValueField(probabilities=segregationProbabilities)
    secgen_segregation2_2 = headfake.field.IfElseField(name="secgen_segregation2_2", condition=condition, true_value=true_value, false_value=None)
    
    # LOCATION OF THE VCF FILE
    vcfLocProbabilities = {"IMGGE Server":0.8, None:0.2}
    loc_vcf = headfake.field.OptionValueField(name="loc_vcf", probabilities=vcfLocProbabilities)
    
    # OTHER LOCATION OF THE VCF FILE
    vcfOtherLocProbabilities = {"External HD":0.8, None:0.2}
    oth_loc_vcf = headfake.field.OptionValueField(name="other_loc_vcf", probabilities=vcfOtherLocProbabilities)
    
    # TODO: Unknown metadata
    oth_loc_vcf_ext = headfake.field.ConstantField(name="other_loc_vcf_ext", value=None)
    
    # TODO: Unknown metadata
    comment = headfake.field.ConstantField(name="comment", value=None)


    # Create fieldset
    fs = headfake.Fieldset(fields=[patientID, diagnosisName, diagnosis_omimID, gene_omimID, icd10Code, dateOfBirth, countryOfBirth, sex,
                            ageWhenTested, testedYears, testedMonths, testedsummary, hasConsanguinity, consanguinityRelation, 
                            hasAffectedRelatives, affectedRelatives, hasSpontaneousAbortions, numberOfAbortions, inVitroFertilization,
                            hpo_descriptive, hpo_data, ageOnset, onsetYears, onsetMonths, onsetSummary, hasID, 
                            testResult,IQTestDescription,otherIdTest, otherIdTestName, otherIDTestResult, hasMetabolicDisease, 
                            hasMetabolicMeasures, hasCT, hasMR, hasEEG, clinic, hasPreviouskaryotype, karyotype_result, hasPreviousMicroarray, 
                            microarrayResult, microarray_sig, microarray_type, microarray_reg_no, microarray_length, microarray_details, hasPreviousOther,
                            otherType, otherResult, ngsTestResult, ngsTestApproach, ngsTestPlatform, ngsTestOtherPlatform, refGenome, gen_gen1, 
                            gen_chromosome1, gen_novar1, variantInheritance1, zygosity1, gen_transcript1, gen_varnamecdna1_1, gen_varnameprot1_1, 
                            segregation1, gen_varnamecdna1_2, gen_varnameprot1_2, segregation_result1_2, secgen2, secgen_gene2, secgen_chromosome2, 
                            secgen_novar2, secgen_inheritance2, secgen_zigosity2, secgen_transcript2, secgen_varnamecdna2_1, secgen_varnameprot2_1,
                            secgen_segregation2_1, secgen_varnamecdna2_2, secgen_varnameprot2_2, secgen_segregation2_2, loc_vcf, oth_loc_vcf, oth_loc_vcf_ext, comment])
    
    return fs

def main():
    if len(sys.argv) < 2:
        print("Ussage: py IMGGE.py <num_rows> <output_file_name>")
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