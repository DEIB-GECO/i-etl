import datetime
import operator

import headfake
import headfake.field
import headfake.transformer
import pandas as pd
import scipy

from constants.datageneration.imgge import ID_PROBABILITIES, OTHER_PROBABILITIES, OTHER_ID_RESULT_PROBABILITIES, \
    METABOLIC_PROBABILITIES, METABOLIC_MEASURES_PROBABILITIES, CT_PROBABILITIES, MR_PROBABILITIES, EEG_PROBABILITIES, \
    KARYOTYPE_PROBABILITIES, MICRO_ARRAY_PROBABILITIES, MICRO_ARRAY_RESULT_PROBABILITIES, \
    MICRO_ARRAY_TYPE_PROBABILITIES, PREVIOUS_PROBABILITIES, OTHER_GENETIC_PROBABILITIES, OTHER_RESULT_PROBABILITIES, \
    GEN_PROBABILITIES, APPROACH_PROBABILITIES, PLATFORM_PROBABILITIES, OTHER_PLATFORM_PROBABILITIES, REF_PROBABILITIES, \
    NO_VAR_PROBABILITIES, INHERITANCE_PROBABILITIES, ZYGOSITY_PROBABILITIES, SEGREGATION_PROBABILITIES, \
    OTHER_AFFECTED_GENES, VCF_LOC_PROBABILITIES, VCF_OTHER_LOC_PROBABILITIES
from database.Execution import Execution
from generators.DataGenerator import DataGenerator
from generators.modules.Diagnosis import Diagnosis
from generators.modules.Gene import Gene
from generators.modules.GeneticTest import Karyotype, Microarray
from generators.modules.Patient import Patient, Age
from generators.modules.Variant import Variant
from utils.file_utils import get_ground_data


# READ COMPLEMENTARY DATASETS


class GeneratorImgge(DataGenerator):
    def __init__(self, execution: Execution):
        super().__init__(execution=execution)
        self.genes_to_phenotype = pd.read_csv(get_ground_data("genes_to_phenotype.txt"), sep="\t")
        self.countries = pd.read_csv(get_ground_data("countries.csv"), sep=",")
        self.diseases = pd.read_csv(get_ground_data("IMGGE_Diseases.csv"), sep=",")

    def generate(self):
        num_rows = self.execution.nb_rows

        fieldset = self.generate_fields()
        hf = headfake.HeadFake.from_python({"fieldset": fieldset})

        # Create the dataset
        data = hf.generate(num_rows=num_rows)
        self.save_generated_file(df=data, filename="data.csv")

    def generate_fields(self):
        # PATIENT ID
        idGenerator = headfake.field.IncrementIdGenerator(min_value=1, length=3)
        patientID = headfake.field.IdField(name="record_id", prefix="RFZO ", generator=idGenerator)

        # DIAGNOSIS NAME
        diagnosisName = headfake.field.MapFileField(mapping_file=get_ground_data("IMGGE_Diseases.csv"), key_field="disease_name",
                                                    name="diagnosis")

        # DIAGNOSIS OMIM IDENTIFIER
        diagnosis_omimID = headfake.field.LookupMapFileField(lookup_value_field="OMIM_Phenotype",
                                                             map_file_field="diagnosis", name="omim_pheno")

        # GENE OMIM IDENTIFIER
        gene_omimID = headfake.field.LookupMapFileField(lookup_value_field="OMIM_Gene", map_file_field="diagnosis",
                                                        name="omim_gene_locus")

        # DIAGNOSIS ICD-10 CODE
        icd10Code = headfake.field.LookupMapFileField(lookup_value_field="ICD_10", map_file_field="diagnosis",
                                                      name="icd10_ref_diagnosis")

        # DATE OF BIRTH
        dateOfBirth = headfake.field.DateOfBirthField(name="dob", distribution=scipy.stats.uniform, mean=0, sd=18,
                                                      min=0, max=0, date_format="%d.%m.%Y")

        # COUNTRY OF BIRTH
        countryOfBirth = headfake.field.MapFileField(mapping_file=get_ground_data("countries.csv"), key_field="Name",
                                                     name="country_birth")

        # SEX PROBABILITIES
        sexProbabilites = {"1": 0.4, "2": 0.5, "99": 0.1}  # {MALE, FEMALE, INDETERMINATE SEX}
        sex = headfake.field.OptionValueField(probabilities=sexProbabilites, name="sex")

        # NUMBER OF YEARS WHEN TESTED
        dobFieldValue = headfake.field.LookupField(field="dob", hidden=True)
        currentAge = headfake.field.derived.AgeField(from_value=dobFieldValue, to_value=datetime.datetime.now(),
                                                     from_format="%d.%m.%Y", to_format="%d.%m.%Y", hidden=True)
        testedYears = headfake.field.OperationField(name="age_number_y", operator=operator.floordiv,
                                                    first_value=currentAge, second_value=2)

        # NUMBER OF MONTHS WHEN TESTED
        testedMonths = headfake.field.OperationField(name="age_number_m", operator=None, first_value=dobFieldValue,
                                                     second_value=datetime.datetime.now(),
                                                     operator_fn=Age.get_months_difference)

        # AGE WHEN TESTED IN YEARS
        testedsummary = headfake.field.OperationField(name="age_summary", operator=None, first_value=testedYears,
                                                      second_value=testedMonths, operator_fn=Age.get_age_summary)

        # AGE WHEN SENT FOR GENETIC TESTING
        ageWhenTested = headfake.field.OperationField(name="age_sent_for_genet", operator=None, first_value=testedYears,
                                                      second_value=testedMonths, operator_fn=Age.age_to_string)

        # THERE IS CONSANGUINITY
        consanguinityProbabilities = {"1": 0.8, "2": 0.1, "99": 0.1}  # {No, Yes, Not applicable}
        hasConsanguinity = headfake.field.OptionValueField(probabilities=consanguinityProbabilities,
                                                           name="consangui_offsp")

        # CONSANGUINEOUS RELATIVES
        offspringProbabilities = {"First": 0.8, "Second": 0.2}
        condition = headfake.field.Condition(field="consangui_offsp", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=offspringProbabilities)
        consanguinityRelation = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value="no",
                                                           name="consangui_offsp_rel")

        # HAS AFFECTED RELATIVES
        affectedRelativesProbabilities = {"1": 0.6, "2": 0.3, "99": 0.1}  # {No, Yes, Unknown}
        hasAffectedRelatives = headfake.field.OptionValueField(probabilities=affectedRelativesProbabilities,
                                                               name="relatives")

        # AFFECTED RELATIVES
        relativesProbabilities = {"Mother": 0.2, "Father": 0.2, "Brother": 0.1, "Sister": 0.1, "Uncle": 0.1,
                                  "Aunt": 0.1, "Grandfather": 0.1, "Grandmother": 0.1}
        condition = headfake.field.Condition(field="relatives", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=relativesProbabilities)
        affectedRelatives = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None,
                                                       name="relative_rel")

        # HAS SPONTANEOUS ABORTIONS
        abortionProbabilities = {"1": 0.6, "2": 0.3, "99": 0.1}  # {No, Yes, Unknown}
        hasSpontaneousAbortions = headfake.field.OptionValueField(probabilities=abortionProbabilities,
                                                                  name="sp_abortion")

        # NUMBER OF SPONTANEOUS ABORTIONS
        numAbortionsProbabilities = {"1": 0.5, "2": 0.2, "3": 0.2, "4": 0.1}
        condition = headfake.field.Condition(field="sp_abortion", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=numAbortionsProbabilities)
        numberOfAbortions = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None,
                                                       name="sp_abortion_numb")

        # IS IN VITRO FERTILIZATION
        ivProbabilities = {"1": 0.7, "2": 0.2, "99": 0.1}  # {No, Yes, Unknown}
        inVitroFertilization = headfake.field.OptionValueField(probabilities=ivProbabilities, name="ivf")

        # LIST OF SYMPTOMS WITH HPO IDENTIFIER
        omimID = headfake.field.OperationField(operator=None, first_value=diagnosis_omimID, second_value=None,
                                               operator_fn=Diagnosis.transform_omimID, hidden=True)
        hpo_descriptive = headfake.field.OperationField(name="hpo_descriptive", operator=None, first_value=omimID,
                                                        second_value=5, operator_fn=Diagnosis.get_symptoms_by_omim_id)

        # LIST OF SYMPTOMS HPO IDENTIFIERS
        hpo_data = headfake.field.OperationField(name="hpo_data", operator=None, first_value=hpo_descriptive,
                                                 second_value=None, operator_fn=Diagnosis.get_hpo_identifiers)

        # YEARS AT ONSET
        # TODO: Create different than testing
        onsetYears = headfake.field.OperationField(name="age_onset_number_y", operator=operator.floordiv,
                                                   first_value=currentAge, second_value=2)

        # MONTHS AT ONSET
        # TODO: Create different than testing
        onsetMonths = headfake.field.OperationField(name="age_onset_number_m", operator=None, first_value=dobFieldValue,
                                                    second_value=datetime.datetime.now(),
                                                    operator_fn=Age.get_months_difference)

        # AGE AT ONSET IN YEARS
        onsetSummary = headfake.field.OperationField(name="age_onset_summary", operator=None, first_value=testedYears,
                                                     second_value=testedMonths, operator_fn=Age.get_age_summary)

        # AGE AT ONSET (DESCRIPTIVE)
        ageOnset = headfake.field.OperationField(name="age_onset", operator=None, first_value=onsetYears,
                                                 second_value=onsetMonths, operator_fn=Age.age_to_string)

        # HAS INTELECTUAL DISABILITY
        hasID = headfake.field.OptionValueField(probabilities=ID_PROBABILITIES, name="intel_dis")

        # IQ TEST RESULT
        condition = headfake.field.Condition(field="intel_dis", operator=operator.eq, value="2")
        transformer = headfake.transformer.ConvertToNumber(as_integer=True)
        true_value = headfake.field.NumberField(distribution=scipy.stats.norm, mean=50, sd=30, min=10, max=70,
                                                transformers=[transformer])
        testResult = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value="no",
                                                name="iq_test")

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
        false_value4 = headfake.field.IfElseField(condition=condition4, true_value=true_value4,
                                                  false_value=false_value5)
        false_value3 = headfake.field.IfElseField(condition=condition3, true_value=true_value3,
                                                  false_value=false_value4)
        false_value2 = headfake.field.IfElseField(condition=condition3, true_value=true_value3,
                                                  false_value=false_value3)
        false_value1 = headfake.field.IfElseField(condition=condition2, true_value=true_value2,
                                                  false_value=false_value2)

        condition = headfake.field.Condition(field="itel_dis", operator=operator.eq, value="2")
        true_value = headfake.field.IfElseField(condition=condition1, true_value=true_value1, false_value=false_value1)
        IQTestDescription = headfake.field.IfElseField(name="iq_test_desc", condition=condition, true_value=true_value,
                                                       false_value="no")

        # HAS OTHER ID TEST
        condition = headfake.field.Condition(field="intel_dis", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=OTHER_PROBABILITIES)
        otherIdTest = headfake.field.IfElseField(name="oth_iq_test_yesno", condition=condition, true_value=true_value,
                                                 false_value="1")

        # OTHER ID TEST NAME
        condition = headfake.field.Condition(field="oth_iq_test_yesno", operator=operator.eq, value="2")
        true_value = headfake.field.MapFileField(mapping_file=get_ground_data("intelligence_tests.csv"), key_field="Name")
        otherIdTestName = headfake.field.IfElseField(name="oth_iq_test_name", condition=condition,
                                                     true_value=true_value, false_value=None)

        # OTHER ID TEST RESULT
        condition = headfake.field.Condition(field="oth_iq_test_yesno", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=OTHER_ID_RESULT_PROBABILITIES)
        otherIDTestResult = headfake.field.IfElseField(name="oth_iq_test_result", condition=condition,
                                                       true_value=true_value, false_value="no")

        # HAS METABOLIC DISEASE
        hasMetabolicDisease = headfake.field.OptionValueField(name="metab_disease",
                                                              probabilities=METABOLIC_PROBABILITIES)

        # THERE ARE METABOLIC TEST MEASURES
        condition = headfake.field.Condition(field="metab_disease", operator=operator.eq, value="2")

        true_value = headfake.field.OptionValueField(probabilities=METABOLIC_MEASURES_PROBABILITIES)
        hasMetabolicMeasures = headfake.field.IfElseField(name="metab_disease_measure", condition=condition,
                                                          true_value=true_value, false_value="1")

        # HAS COMPUTED TOMOGRAPHY
        hasCT = headfake.field.OptionValueField(name="ct_scan", probabilities=CT_PROBABILITIES)

        # HAS MAGNETIC RESONANCE
        hasMR = headfake.field.OptionValueField(name="mr_scan", probabilities=MR_PROBABILITIES)

        # HAS EEG
        hasEEG = headfake.field.OptionValueField(name="eeg", probabilities=EEG_PROBABILITIES)

        # CLINICAL CENTER OF REFERRAL
        country_value = headfake.field.LookupField(field="country_birth")
        clinic = headfake.field.OperationField(name="clinic", operator=None, first_value=country_value,
                                               second_value=None, operator_fn=Patient.generate_hospital_name_by_country)

        # HAS PREVIOUS KARYOTYPE
        hasPreviouskaryotype = headfake.field.OptionValueField(name="karyotype", probabilities=KARYOTYPE_PROBABILITIES)

        # PREVIOUS KARYOTYPE RESULT
        condition = headfake.field.Condition(field="karyotype", operator=operator.eq, value="2")
        true_value = headfake.field.OperationField(operator=None, first_value=sex, second_value=None,
                                                   operator_fn=Karyotype.generate_karyotype_result)
        karyotype_result = headfake.field.IfElseField(name="karyotype_result", condition=condition,
                                                      true_value=true_value, false_value="no")

        # HAS PREVIOUS MICROARRAY
        hasPreviousMicroarray = headfake.field.OptionValueField(name="microarray",
                                                                probabilities=MICRO_ARRAY_PROBABILITIES)

        # PREVIOUS MICROARRAY RESULT
        condition = headfake.field.Condition(field="microarray", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=MICRO_ARRAY_RESULT_PROBABILITIES)
        microarrayResult = headfake.field.IfElseField(name="microarray_result", condition=condition,
                                                      true_value=true_value, false_value=None)

        # MICROARRAY RESULT SIGNIFICANCE
        true_value = headfake.field.OperationField(operator=None, first_value=microarrayResult, second_value=None,
                                                   operator_fn=Microarray.get_microarray_significance)
        microarray_sig = headfake.field.IfElseField(name="microarray_sig", condition=condition, true_value=true_value,
                                                    false_value=None)

        # MICROARRAY RESULT TYPE
        microarray_type = headfake.field.OptionValueField(name="microarray_type",
                                                          probabilities=MICRO_ARRAY_TYPE_PROBABILITIES)

        # MICROARRAY RESULT REGION
        true_value = headfake.field.OperationField(operator=None, first_value=None, second_value=None,
                                                   operator_fn=Microarray.generate_cytogenetic_location)
        microarray_reg_no = headfake.field.IfElseField(name="microarray_reg_no", condition=condition,
                                                       true_value=true_value, false_value=None)

        # MICROARRAY RESULT LENGTH
        true_value = headfake.field.OperationField(operator=None, first_value=None, second_value=None,
                                                   operator_fn=Microarray.generate_microarray_length)
        microarray_length = headfake.field.IfElseField(name="microarray_length", condition=condition,
                                                       true_value=true_value, false_value=None)

        # MICROARRAY RESULT DETAILS
        true_value = headfake.field.OperationField(operator=None, first_value=microarray_reg_no,
                                                   second_value=microarray_type,
                                                   operator_fn=Microarray.generate_microarray_details)
        microarray_details = headfake.field.IfElseField(name="microarray_details", condition=condition,
                                                        true_value=true_value, false_value=None)

        # HAS OTHER PREVIOUS GENETIC TEST
        hasPreviousOther = headfake.field.OptionValueField(name="other_gen_test", probabilities=PREVIOUS_PROBABILITIES)

        # OTHER GENETIC TEST TYPE
        condition = headfake.field.Condition(field="other_gen_test", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=OTHER_GENETIC_PROBABILITIES)
        otherType = headfake.field.IfElseField(name="other_gen_test_type", condition=condition, true_value=true_value,
                                               false_value=None)

        # OTHER GENETIC TEST RESULT
        condition = headfake.field.Condition(field="other_gen_test", operator=operator.eq, value="2")
        true_value = headfake.field.OptionValueField(probabilities=OTHER_RESULT_PROBABILITIES)
        otherResult = headfake.field.IfElseField(name="other_gen_test_typeresult", condition=condition,
                                                 true_value=true_value, false_value=None)

        # GENETIC TEST RESULT
        ngsTestResult = headfake.field.OptionValueField(name="gen_result", probabilities=GEN_PROBABILITIES)

        # GENETIC TEST APPROACH
        ngsTestApproach = headfake.field.OptionValueField(name="gen_result", probabilities=APPROACH_PROBABILITIES)

        # SEQUENCING PLATFORM
        ngsTestPlatform = headfake.field.OptionValueField(name="gen_ngsplatform", probabilities=PLATFORM_PROBABILITIES)

        # OTHER SEQUENCING PLATFORM
        condition = headfake.field.Condition(field="gen_ngsplatform", operator=operator.eq, value=None)

        true_value = headfake.field.OptionValueField(probabilities=OTHER_PLATFORM_PROBABILITIES)
        ngsTestOtherPlatform = headfake.field.IfElseField(name="oth_gen_ngsplatform", condition=condition,
                                                          true_value=true_value, false_value=None)

        # REFERENCE GENOME
        refGenome = headfake.field.OptionValueField(name="gen_ref", probabilities=REF_PROBABILITIES)

        # GENE 1 NAME
        # TODO: THERE CAN BE MORE THAN ONE GENE
        gen_gen1 = headfake.field.LookupMapFileField(name="gen_gen1", lookup_value_field="Gene",
                                                     map_file_field="diagnosis")

        # CHROMOSOME NAME
        gen_chromosome1 = headfake.field.OperationField(name="gen_chromosome1", operator=None, first_value=None,
                                                        second_value=None, operator_fn=Gene.generate_chromosome)

        # NUMBER OF VARIANTS FOUND IN GENE 1
        gen_novar1 = headfake.field.OptionValueField(name="gen_novar1", probabilities=NO_VAR_PROBABILITIES)

        # VARIANT INHERITANCE IN GENE 1
        variantInheritance1 = headfake.field.OptionValueField(name="gen_inheritance1",
                                                              probabilities=INHERITANCE_PROBABILITIES)

        # VARIANT 1 ZYGOSITY
        zygosity1 = headfake.field.OptionValueField(name="gen_zigosity1", probabilities=ZYGOSITY_PROBABILITIES)

        # TRANSCRIPT IDENTIFIER OF GENE 1
        # TODO: Change to random for the generation of random patches
        generator = headfake.field.RandomReuseIdGenerator(length=6, min_value=1645)
        gen_transcript1 = headfake.field.IdField(name="gen_transcript1", prefix='NM_', suffix=".4", generator=generator)

        # CODING VARIANT 1 NAME
        # TODO: Create a HGVS name generator
        gen_varnamecdna1_1 = headfake.field.MapFileField(mapping_file=get_ground_data("variants.csv"), key_field="coding_name",
                                                         name="gen_varnamecdna1_1")

        # PROTEIN VARIANT 1 NAME
        # TODO: Create a HGVS name generator
        gen_varnameprot1_1 = headfake.field.LookupMapFileField(name="gen_varnameprot1_1",
                                                               lookup_value_field="protein_name",
                                                               map_file_field="gen_varnamecdna1_1")

        # VARIANT 1 SEGREGATION
        segregation1 = headfake.field.OptionValueField(name="segregation_result1_1",
                                                       probabilities=SEGREGATION_PROBABILITIES)

        # CODING VARIANT 2 NAME
        # TODO: Create a HGVS name generator
        condition = headfake.field.Condition(field="gen_novar1", operator=operator.eq, value=2)
        true_value = headfake.field.MapFileField(mapping_file=get_ground_data("variants.csv"), key_field="coding_name",
                                                 name="gen_varnamecdna1_2")
        gen_varnamecdna1_2 = headfake.field.IfElseField(name="gen_varnamecdna1_2", condition=condition,
                                                        true_value=true_value, false_value=None)

        # PROTEIN VARIANT 2 NAME
        # TODO: Create a HGVS name generator
        condition = headfake.field.Condition(field="gen_novar1", operator=operator.eq, value=2)
        true_value = headfake.field.OperationField(operator=None, first_value=gen_varnamecdna1_2, second_value=None,
                                                   operator_fn=Variant.get_protein_name)
        gen_varnameprot1_2 = headfake.field.IfElseField(name="gen_varnameprot1_2", condition=condition,
                                                        true_value=true_value, false_value=None)

        # VARIANT 2 SEGREGATION
        segregation_result1_2 = headfake.field.OptionValueField(name="segregarion_result1_2",
                                                                probabilities=SEGREGATION_PROBABILITIES)

        # THERE ARE OTHER GENES NOT RELATED TO ID OR METABOLIC DISEASE AFFECTED
        secgen2 = headfake.field.OptionValueField(probabilities=OTHER_AFFECTED_GENES, name="secgen2")

        # NAME OF GENE 2
        condition = headfake.field.Condition(field="secgen2", operator=operator.eq, value="Yes")
        true_value = headfake.field.MapFileField(mapping_file=get_ground_data("variants.csv"), key_field="other_gene")
        secgen_gene2 = headfake.field.IfElseField(name="secgen_gene2", condition=condition, true_value=true_value,
                                                  false_value=None)

        # CHROMOSOME 2
        true_value = headfake.field.OperationField(operator=None, first_value=None, second_value=None,
                                                   operator_fn=Gene.generate_chromosome)
        secgen_chromosome2 = headfake.field.IfElseField(name="secgen_chromosome2", condition=condition,
                                                        true_value=true_value, false_value=None)

        # NUMBER OF VARIANTS FOUND IN GENE 2
        true_value = headfake.field.OptionValueField(probabilities=NO_VAR_PROBABILITIES)
        secgen_novar2 = headfake.field.IfElseField(name="secgen_novar2", condition=condition, true_value=true_value,
                                                   false_value=None)

        # VARIANT INHERITANCE REGARDING GENE 2
        true_value = headfake.field.OptionValueField(probabilities=INHERITANCE_PROBABILITIES)
        secgen_inheritance2 = headfake.field.IfElseField(name="secgen_inheritance2", condition=condition,
                                                         true_value=true_value, false_value=None)

        # VARIANT ZYGOSITY REGARDING GENE 2
        true_value = headfake.field.OptionValueField(probabilities=ZYGOSITY_PROBABILITIES)
        secgen_zigosity2 = headfake.field.IfElseField(name="secgen_zigosity2", condition=condition,
                                                      true_value=true_value, false_value=None)

        # TRANSCRIPT IDENTIFIER OF GENE 2
        # TODO: Change to random for the generation of random patches
        generator = headfake.field.RandomReuseIdGenerator(length=6, min_value=1645)
        true_value = headfake.field.IdField(prefix='NM_', suffix=".4", generator=generator)
        secgen_transcript2 = headfake.field.IfElseField(name="secgen_transcript2", condition=condition,
                                                        true_value=true_value, false_value=None)

        # CODING NAME OF VARIANT 1
        true_value = headfake.field.MapFileField(mapping_file=get_ground_data("variants.csv"), key_field="coding_name",
                                                 name="secgen_varnamecdna2_1")
        secgen_varnamecdna2_1 = headfake.field.IfElseField(name="secgen_varnamecdna2_1", condition=condition,
                                                           true_value=true_value, false_value=None)

        # PROTEIN NAME OF VARIANT 1
        true_value = headfake.field.OperationField(operator=None, first_value=secgen_varnamecdna2_1, second_value=None,
                                                   operator_fn=Variant.get_protein_name)
        secgen_varnameprot2_1 = headfake.field.IfElseField(name="secgen_varnameprot2_1", condition=condition,
                                                           true_value=true_value, false_value=None)

        # VARIANT 1 SEGREGATION
        true_value = headfake.field.OptionValueField(probabilities=SEGREGATION_PROBABILITIES)
        secgen_segregation2_1 = headfake.field.IfElseField(name="secgen_segregation2_1", condition=condition,
                                                           true_value=true_value, false_value=None)

        # CODING NAME OF VARIANT 2
        condition = headfake.field.Condition(field="secgen_novar2", operator=operator.eq, value=2)
        true_value = headfake.field.MapFileField(mapping_file=get_ground_data("variants.csv"), key_field="coding_name",
                                                 name="secgen_varnamecdna2_1")
        true_value_2 = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None)
        secgen_varnamecdna2_2 = headfake.field.IfElseField(name="secgen_varnamecdna2_2", condition=condition,
                                                           true_value=true_value_2, false_value=None)

        # PROTEIN NAME OF VARIANT 2
        condition = headfake.field.Condition(field="secgen_novar2", operator=operator.eq, value=2)
        true_value = headfake.field.OperationField(operator=None, first_value=secgen_varnamecdna2_2, second_value=None,
                                                   operator_fn=Variant.get_protein_name)
        true_value_2 = headfake.field.IfElseField(condition=condition, true_value=true_value, false_value=None)
        secgen_varnameprot2_2 = headfake.field.IfElseField(name="gen_varnameprot2_2", condition=condition,
                                                           true_value=true_value_2, false_value=None)

        # VARIANT 2 SEGREGATION
        true_value = headfake.field.OptionValueField(probabilities=SEGREGATION_PROBABILITIES)
        secgen_segregation2_2 = headfake.field.IfElseField(name="secgen_segregation2_2", condition=condition,
                                                           true_value=true_value, false_value=None)

        # LOCATION OF THE VCF FILE
        loc_vcf = headfake.field.OptionValueField(name="loc_vcf", probabilities=VCF_LOC_PROBABILITIES)

        # OTHER LOCATION OF THE VCF FILE
        oth_loc_vcf = headfake.field.OptionValueField(name="other_loc_vcf", probabilities=VCF_OTHER_LOC_PROBABILITIES)

        # TODO: Unknown metadata
        oth_loc_vcf_ext = headfake.field.ConstantField(name="other_loc_vcf_ext", value=None)

        # TODO: Unknown metadata
        comment = headfake.field.ConstantField(name="comment", value=None)

        # Create fieldset
        fs = headfake.Fieldset(
            fields=[patientID, diagnosisName, diagnosis_omimID, gene_omimID, icd10Code, dateOfBirth, countryOfBirth,
                    sex,
                    ageWhenTested, testedYears, testedMonths, testedsummary, hasConsanguinity, consanguinityRelation,
                    hasAffectedRelatives, affectedRelatives, hasSpontaneousAbortions, numberOfAbortions,
                    inVitroFertilization,
                    hpo_descriptive, hpo_data, ageOnset, onsetYears, onsetMonths, onsetSummary, hasID,
                    testResult, IQTestDescription, otherIdTest, otherIdTestName, otherIDTestResult, hasMetabolicDisease,
                    hasMetabolicMeasures, hasCT, hasMR, hasEEG, clinic, hasPreviouskaryotype, karyotype_result,
                    hasPreviousMicroarray,
                    microarrayResult, microarray_sig, microarray_type, microarray_reg_no, microarray_length,
                    microarray_details, hasPreviousOther,
                    otherType, otherResult, ngsTestResult, ngsTestApproach, ngsTestPlatform, ngsTestOtherPlatform,
                    refGenome, gen_gen1,
                    gen_chromosome1, gen_novar1, variantInheritance1, zygosity1, gen_transcript1, gen_varnamecdna1_1,
                    gen_varnameprot1_1,
                    segregation1, gen_varnamecdna1_2, gen_varnameprot1_2, segregation_result1_2, secgen2, secgen_gene2,
                    secgen_chromosome2,
                    secgen_novar2, secgen_inheritance2, secgen_zigosity2, secgen_transcript2, secgen_varnamecdna2_1,
                    secgen_varnameprot2_1,
                    secgen_segregation2_1, secgen_varnamecdna2_2, secgen_varnameprot2_2, secgen_segregation2_2, loc_vcf,
                    oth_loc_vcf, oth_loc_vcf_ext, comment])

        return fs
