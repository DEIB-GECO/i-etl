import headfake
import headfake.field
import headfake.transformer
import scipy
import operator
import sys

import scipy.stats

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

    # age_sent_for_genet
    # TODO: Pending answer from headfake developers
    ageWhenTested = headfake.field.ConstantField(name="age_sent_for_genet", value=None)

    # age_number_y
    # TODO: Pending answer from headfake developers
    testedYears = headfake.field.ConstantField(name="age_number_y", value=None)

    # age_number_m:
    # TODO: Pending answer from headfake developers
    testedMonths = headfake.field.ConstantField(name="age_number_m", value=None)

    # age_summary
    # TODO: Pending answer from headfake developers
    testedsummary = headfake.field.ConstantField(name="age_summary", value=None)

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
      
    # hpo_descriptive
    # TODO:
    hpo_descriptive = headfake.field.ConstantField(name="hpo_descriptive", value=None)

    # hpo_data
    # TODO:
    hpo_data = headfake.field.ConstantField(name="hpo_data", value=None)

    # age_onset
    # TODO: Pending answer from headfake developers
    ange_onset = headfake.field.ConstantField(name="age_onset", value=None)

    # age_onset_number_y
    # TODO: Pending answer from headfake developers
    age_onset_number_y = headfake.field.ConstantField(name="age_onset_number_y", value=None)

    # age_onset_number_m
    # TODO: Pending answer from headfake developers
    age_onset_number_m = headfake.field.ConstantField(name="age_onset_number_m", value=None)

    # age_onset_summary
    # TODO: Pending answer from headfake developers
    age_onset_summary = headfake.field.ConstantField(name="age_onset_summary", value=None)

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
    
    # TODO:
    clinic = headfake.field.ConstantField(name="clinic", value=None)
    
    # TODO:
    karyotype = headfake.field.ConstantField(name="karyotype", value=None)
    
    # TODO:
    karyotype_result = headfake.field.ConstantField(name="karyotype_result", value=None)
    
    # TODO:
    microarray = headfake.field.ConstantField(name="microarray", value=None)
    
    # TODO:
    microarray_result = headfake.field.ConstantField(name="microarray_result", value=None)
    
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
    
    # TODO:
    other_gen_test = headfake.field.ConstantField(name="other_gen_test", value=None)
    
    # TODO:
    other_gen_test_type = headfake.field.ConstantField(name="other_gen_test_ type", value=None)
    
    # TODO:
    other_gen_tests_typeresult = headfake.field.ConstantField(name="other_gen_test_typeresult", value=None)
    
    # TODO:
    gen_result = headfake.field.ConstantField(name="gen_result", value=None)
    
    # TODO:
    gen_approach = headfake.field.ConstantField(name="gen_approach", value=None)
    
    # TODO:
    gen_ngsplatform = headfake.field.ConstantField(name="gen_nsgplatform", value=None)
    
    # TODO:
    oth_gen_ngsplatform = headfake.field.ConstantField(name="oth_gen_ngsplatform", value=None)
   
    # TODO:
    gen_ref = headfake.field.ConstantField(name="gen_ref", value=None)
    
    # TODO:
    gen_gen1 = headfake.field.ConstantField(name="gen_gen1", value=None)
    
    # TODO:
    gen_chromosome1 = headfake.field.ConstantField(name="gen_chromosome1", value=None)
    
    # TODO:
    gen_novar1 = headfake.field.ConstantField(name="gen_novar1", value=None)
    
    # TODO:
    gen_inheritance1 = headfake.field.ConstantField(name="gen_inheritance1", value=None)
    
    # TODO:
    gen_zigosity1 = headfake.field.ConstantField(name="gen_zigosity1", value=None)
    
    # TODO:
    gen_transcript1 = headfake.field.ConstantField(name="gen_transcript1", value=None)
    
    # TODO:
    gen_varnamecdna1_1 = headfake.field.ConstantField(name="gen_varnamecdna1_1", value=None)
    
    # TODO:
    gen_varnameprot1_1 = headfake.field.ConstantField(name="gen_varnameprot1_1", value=None)
    
    # TODO:
    segregation_result1_1 = headfake.field.ConstantField(name="segregarion_result1", value=None)
    
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
    
    # TODO:
    secgen_transcript2 = headfake.field.ConstantField(name="secgen_transcript2", value=None)
    
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
                            hpo_descriptive, hpo_data, ange_onset, age_onset_number_y, age_onset_number_m, age_onset_summary, hasID, 
                            testResult,IQTestDescription, otherIdTest, otherIdTestName, otherIDTestResult, hasMetabolicDisease, 
                            hasMetabolicMeasures, hasCT, hasMR, hasEEG, clinic, karyotype, karyotype_result, microarray, microarray_result,
                            microarray_sig, microarray_type, microarray_reg_no, microarray_lenght, microarray_details, other_gen_test,
                            other_gen_test_type, other_gen_tests_typeresult, gen_result, gen_approach, gen_ngsplatform,
                            oth_gen_ngsplatform, gen_ref, gen_gen1, gen_chromosome1, gen_novar1, gen_inheritance1,
                            gen_zigosity1, gen_transcript1, gen_varnamecdna1_1, gen_varnameprot1_1, segregation_result1_1, gen_varnamecdna1_2,
                            gen_varnameprot1_2, segregation_result1_2, secgen2, secgen_gene2, secgen_chromosome2, secgen_novar2,
                            secgen_inheritance2, secgen_zigosity2, secgen_transcript2, secgen_varnamecdna2_1, secgen_varnameprot2_1,
                            secgen_segregation2_1, secgen_varnamecdna2_2, secgen_varnameprot2_2, secgen_segregation2_2,
                            loc_vcf, oth_loc_vcf, oth_loc_vcf_ext, comment])
    
    return fs

def main():
    if len(sys.argv) < 1:
        print("Ussage: py BETTER_IMGGE_Core_Table.py <output_file_name>")
    else:
        output_file = sys.argv[1]
    
        fieldset = generate_fields()
        hf = headfake.HeadFake.from_python({"fieldset":fieldset})

        # Create the dataset
        data = hf.generate(num_rows=10)

        # Create CSV output
        data.to_csv(output_file, index=False)
    
        print(f"Results written to {output_file}")

if __name__ == "__main__":
    main()