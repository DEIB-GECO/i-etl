import operator
import random

import headfake
import headfake.field
import headfake.transformer
import pandas as pd
import scipy
import scipy.stats

from constants.datageneration.buzzi import THREE_VALUE_OPTIONS, ANSWER_IX, BIRTH_METHOD, TWO_VALUE_OPTIONS, \
    SAMPLE_QUALITY
from database.Execution import Execution
from generators.DataGenerator import DataGenerator
from generators.modules.MetabolicTest import MetabolicTest, Measure
from generators.modules.Patient import BirthData, GeographicData, Patient
from utils.file_utils import get_ground_data


class GeneratorBuzzi(DataGenerator):
    LOCALE = "it_IT"

    def __init__(self, execution: Execution):
        super().__init__(execution=execution)

    def generate(self):
        num_rows = self.execution.nb_rows
        fieldset = self.generate_screening_dataset()
        hf = headfake.HeadFake.from_python({"fieldset": fieldset})

        # Create the screening dataset
        data_1 = hf.generate(num_rows=num_rows)
        self.save_generated_file(df=data_1, filename="screening.csv")

        # Create the diagnosis dataset
        data_2 = self.generate_diagnosis_dataset(data_1["id"])
        self.save_generated_file(df=data_2, filename="diagnoses-latest.xlsx")

    def generate_screening_dataset(self):
        # argininosuccinic acid
        asaTotal = headfake.field.OperationField(name="ASATotal", operator=None, first_value="argininosuccinic acid",
                                                 second_value=None, operator_fn=Measure.generate_measure_value)

        # alanine
        ala = headfake.field.OperationField(name="Ala", operator=None, first_value="alanine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # genetic variant 1
        allele1 = headfake.field.MapFileField(name="Allele 1", mapping_file=get_ground_data("variants.csv"),
                                              key_field="coding_name")

        # genetic variant 2
        allele2 = headfake.field.MapFileField(name="Allele 2", mapping_file=get_ground_data("variants.csv"),
                                              key_field="coding_name")

        # arginine
        arg = headfake.field.OperationField(name="Arg", operator=None, first_value="arginine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # citrulline
        cit = headfake.field.OperationField(name="Cit", operator=None, first_value="citrulline", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # glutamic acid
        glu = headfake.field.OperationField(name="Glu", operator=None, first_value="glutamic acid", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # glycine
        gly = headfake.field.OperationField(name="Gly", operator=None, first_value="glycine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # leucine\isoleucine\hydroxyproline
        leu_ile_pro_oh = headfake.field.OperationField(name="Leu\\Ile\\Pro-OH", operator=None,
                                                       first_value="leucine_isoleucine_hydroxyproline",
                                                       second_value=None, operator_fn=Measure.generate_measure_value)

        # methionine
        met = headfake.field.OperationField(name="MET", operator=None, first_value="methionine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # ornithine
        orn = headfake.field.OperationField(name="Orn", operator=None, first_value="ornithine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # phenylalanine
        phe = headfake.field.OperationField(name="PHE", operator=None, first_value="phenylalanine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # proline
        pro = headfake.field.OperationField(name="Pro", operator=None, first_value="proline", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # tyrosine
        tyr = headfake.field.OperationField(name="TYR", operator=None, first_value="tyrosine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # valina
        val = headfake.field.OperationField(name="Val", operator=None, first_value="valina", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # human biotinidase activity
        btd = headfake.field.OperationField(name="BTD", operator=None, first_value="human biotinidase activity",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # free carnitine
        c0 = headfake.field.OperationField(name="C0", operator=None, first_value="free carnitine", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # acetylcarnitine
        c2 = headfake.field.OperationField(name="C2", operator=None, first_value="acetylcarnitine", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # propionylcarnitine
        c3 = headfake.field.OperationField(name="C3", operator=None, first_value="propionylcarnitine",
                                           second_value=None, operator_fn=Measure.generate_measure_value)

        # malonylcarnitine\3-hydroxy-butyrylcarnitine
        c3dc_c4oh = headfake.field.OperationField(name="C3DC\\C4OH", operator=None,
                                                  first_value="malonylcarnitine_3-hydroxy-butyrylcarnitine",
                                                  second_value=None, operator_fn=Measure.generate_measure_value)

        # butyrylcarnitine
        c4 = headfake.field.OperationField(name="C4", operator=None, first_value="butyrylcarnitine", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # methylmalonyl\3-hydroxy-isovalerylcarnitine
        c4dc_c5oh = headfake.field.OperationField(name="C4DC\\C5OH", operator=None,
                                                  first_value="methylmalonyl_3-hydroxy-isovalerylcarnitine",
                                                  second_value=None, operator_fn=Measure.generate_measure_value)

        # isovalerylcarnitine
        c5 = headfake.field.OperationField(name="C5", operator=None, first_value="isovalerylcarnitine",
                                           second_value=None, operator_fn=Measure.generate_measure_value)

        # tiglylcarnitine
        c5_1 = headfake.field.OperationField(name="C5:1", operator=None, first_value="tiglylcarnitine",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # glutarylcarnitine\3-hydroxy-hexanoylcarnitine
        c5dc_c6oh = headfake.field.OperationField(name="C5DC\\C6OH", operator=None,
                                                  first_value="glutarylcarnitine_3-hydroxy-hexanoylcarnitine",
                                                  second_value=None, operator_fn=Measure.generate_measure_value)

        # hexanoylcarnitine
        c6 = headfake.field.OperationField(name="C6", operator=None, first_value="hexanoylcarnitine", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # adipylcarnitine
        c6dc = headfake.field.OperationField(name="C6DC", operator=None, first_value="adipylcarnitine",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # octanoylcarnitine
        c8 = headfake.field.OperationField(name="C8", operator=None, first_value="octanoylcarnitine", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # octenoylcarnitine
        c8_1 = headfake.field.OperationField(name="C8:1", operator=None, first_value="octenoylcarnitine",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # decanoylcarnitine
        c10 = headfake.field.OperationField(name="C10", operator=None, first_value="decanoylcarnitine",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # decenoylcarnitine
        c10_1 = headfake.field.OperationField(name="C10:1", operator=None, first_value="decenoylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # decayenoylcarnitine
        c10_2 = headfake.field.OperationField(name="C10:2", operator=None, first_value="decayenoylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # dodecanoylcarnitine
        c12 = headfake.field.OperationField(name="C12", operator=None, first_value="dodecanoylcarnitine",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # dodecenoylcarnitine
        c12_1 = headfake.field.OperationField(name="C12:1", operator=None, first_value="dodecenoylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # tetradecanoylcarnitine (myristoylcarnitine)
        c14 = headfake.field.OperationField(name="C14", operator=None,
                                            first_value="tetradecanoylcarnitine (myristoylcarnitine)",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # tetradecenoylcarnitine
        c14_1 = headfake.field.OperationField(name="C14:1", operator=None, first_value="tetradecenoylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # tetradecadienoylcarnitine
        c14_2 = headfake.field.OperationField(name="C14:2", operator=None, first_value="tetradecadienoylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # 3-hydroxy-tetradecanoylcarnitine
        c14_oh = headfake.field.OperationField(name="C14-OH", operator=None,
                                               first_value="3-hydroxy-tetradecanoylcarnitine", second_value=None,
                                               operator_fn=Measure.generate_measure_value)

        # hexadecanoylcarnitine
        c16 = headfake.field.OperationField(name="C16", operator=None, first_value="hexadecanoylcarnitine",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # hexadecenoylcarnitine
        c16_1 = headfake.field.OperationField(name="C16:1", operator=None, first_value="hexadecenoylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # 3-hydroxy-hexadecanoylcarnitine
        c16_oh = headfake.field.OperationField(name="C16:OH", operator=None,
                                               first_value="3-hydroxy-hexadecanoylcarnitine", second_value=None,
                                               operator_fn=Measure.generate_measure_value)

        # 3-hydroxy-hexadecenoylcarnitine
        c16_1_oh = headfake.field.OperationField(name="C16:1-OH", operator=None,
                                                 first_value="3-hydroxy-hexadecanoylcarnitine", second_value=None,
                                                 operator_fn=Measure.generate_measure_value)

        # octadecanoylcarnitine (stearoylcarnitine)
        c18 = headfake.field.OperationField(name="C18", operator=None,
                                            first_value="octadecanoylcarnitine (stearoylcarnitine)", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # octadecenoylcarnitine (oleylcarnitine)
        c18_1 = headfake.field.OperationField(name="C18:1", operator=None,
                                              first_value="octadecenoylcarnitine (oleylcarnitine)", second_value=None,
                                              operator_fn=Measure.generate_measure_value)

        # octadecadienoylcarnitine (linoleylcarnitine)
        c18_2 = headfake.field.OperationField(name="C18:2", operator=None,
                                              first_value="octadecadienoylcarnitine (linoleylcarnitine)",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # 3-hydroxy-octadecanoylcarnitine
        c18_oh = headfake.field.OperationField(name="C18-OH", operator=None,
                                               first_value="3-hydroxy-octadecanoylcarnitine", second_value=None,
                                               operator_fn=Measure.generate_measure_value)

        # 3-hydroxy-octacedecenoylcarnitine
        c18_1_oh = headfake.field.OperationField(name="C18:1-OH", operator=None,
                                                 first_value="3-hydroxy-octacedecenoylcarnitine", second_value=None,
                                                 operator_fn=Measure.generate_measure_value)

        # 3-hydroxy-octadecadienoylcarnitine
        c18_2oh = headfake.field.OperationField(name="C18:2-OH", operator=None,
                                                first_value="3-hydroxy-octadecadienoylcarnitine", second_value=None,
                                                operator_fn=Measure.generate_measure_value)

        # eicosanoylcarnitine (peanutylcarnitine)
        c20 = headfake.field.OperationField(name="C20", operator=None,
                                            first_value="eicosanoylcarnitine (peanutylcarnitine)", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # docosanoylcarnitine (beenonylcarnitine)
        c22 = headfake.field.OperationField(name="C22", operator=None,
                                            first_value="docosanoylcarnitine (beenonylcarnitine)", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # tetracosanoylcarnitine (lignoceroylcarnitine)
        c24 = headfake.field.OperationField(name="C24", operator=None,
                                            first_value="tetracosanoylcarnitine (lignoceroylcarnitine)",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # hexacosanoylcarnitine (cerotolcarnitine)
        c26 = headfake.field.OperationField(name="C26", operator=None,
                                            first_value="hexacosanoylcarnitine (cerotolcarnitine)", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # succinylacetone
        sa = headfake.field.OperationField(name="SA", operator=None, first_value="succinylacetone", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # adenosine
        ado = headfake.field.OperationField(name="ADO", operator=None, first_value="adenosine", second_value=None,
                                            operator_fn=Measure.generate_measure_value)

        # 2\'-deoxyadenosine
        d_ado = headfake.field.OperationField(name="D-ADO", operator=None, first_value="2_deoxyadenosine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # C20:0 lysophosphatidylcholine
        c20_0_lpc = headfake.field.OperationField(name="C20:0-LPC", operator=None,
                                                  first_value="C20:0 lysophosphatidylcholine", second_value=None,
                                                  operator_fn=Measure.generate_measure_value)

        # C22:0 lysophosphatidylcholine
        c22_0_lpc = headfake.field.OperationField(name="C22:0-LPC", operator=None,
                                                  first_value="C22:0 lysophosphatidylcholine", second_value=None,
                                                  operator_fn=Measure.generate_measure_value)

        # C24:0 lysophosphatidylcholine
        c24_0_lpc = headfake.field.OperationField(name="C24:0-LPC", operator=None,
                                                  first_value="C24:0 lysophosphatidylcholine", second_value=None,
                                                  operator_fn=Measure.generate_measure_value)

        # C26:0 lysophosphatidylcholine
        c26_0_lpc = headfake.field.OperationField(name="C26:0-LPC", operator=None,
                                                  first_value="C26:0 lysophosphatidylcholine", second_value=None,
                                                  operator_fn=Measure.generate_measure_value)

        # human thyroid stimulating hormone
        s_tsh = headfake.field.OperationField(name="s-TSH", operator=None,
                                              first_value="human thyroid stimulating hormone", second_value=None,
                                              operator_fn=Measure.generate_measure_value)

        # human immunoreactive trypsin(ogen)
        irt_gsp = headfake.field.OperationField(name="IRT-GSP", operator=None,
                                                first_value="human immunoreactive trypsin(ogen)", second_value=None,
                                                operator_fn=Measure.generate_measure_value)

        # total galactose (galactose and galactose 1-phospate)
        tgal = headfake.field.OperationField(name="TGAL", operator=None,
                                             first_value="total galactose (galactose and galactose 1-phospate)",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # methylmalonic acid
        mma = headfake.field.OperationField(name="MMA", operator=None, first_value="methylmalonic acid",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # ethylmalonic acid
        ema = headfake.field.OperationField(name="EMA", operator=None, first_value="ethylmalonic acid",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # glutaric acid
        ga = headfake.field.OperationField(name="GEA", operator=None, first_value="glutaric acid", second_value=None,
                                           operator_fn=Measure.generate_measure_value)

        # 2-OH glutaric acid
        _2oh_ga = headfake.field.OperationField(name="2OH GA", operator=None, first_value="2-OH glutaric acid",
                                                second_value=None, operator_fn=Measure.generate_measure_value)

        # 3-OH glutaric acid
        _3oh_ga = headfake.field.OperationField(name="3OH GA", operator=None, first_value="3-OH glutaric acid",
                                                second_value=None, operator_fn=Measure.generate_measure_value)

        # homocysteine
        hcys = headfake.field.OperationField(name="HCYs", operator=None, first_value="homocysteine", second_value=None,
                                             operator_fn=Measure.generate_measure_value)

        # 3-OH propionic acid
        _3oh_pa = headfake.field.OperationField(name="3OH PA", operator=None, first_value="3-OH propionic acid",
                                                second_value=None, operator_fn=Measure.generate_measure_value)

        # methylcitric acid
        mca = headfake.field.OperationField(name="MCA", operator=None, first_value="methylcitric acid",
                                            second_value=None, operator_fn=Measure.generate_measure_value)

        # orotic acid
        orotico = headfake.field.OperationField(name="OROTICO", operator=None, first_value="orotic acid",
                                                second_value=None, operator_fn=Measure.generate_measure_value)

        # pivaloylcarnitine
        piva = headfake.field.OperationField(name="PIVA", operator=None, first_value="pivaloylcarnitine",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # 2-methylbutyrylcarnitine
        _2mbc = headfake.field.OperationField(name="2MBC", operator=None, first_value="2-methylbutyrylcarnitine",
                                              second_value=None, operator_fn=Measure.generate_measure_value)

        # butyrylcarnitine
        c4_b = headfake.field.OperationField(name="c4-b", operator=None, first_value="butyrylcarnitine",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # isobutyrylcarnitine
        c4_i = headfake.field.OperationField(name="c4-i", operator=None, first_value="isobutyrylcarnitine",
                                             second_value=None, operator_fn=Measure.generate_measure_value)

        # Patient data
        sex = Patient.generate_sex(field_name="Sex", male_value="M", female_value="F")
        city = GeographicData.generate_city(field_name="City", locale=GeneratorBuzzi.LOCALE)
        ethnicity = GeographicData.generate_ethnicity(field_name="Ethnicity", ethnicities=None)

        # Birth data
        dob = BirthData.generate_date_of_birth(field_name="DateOfBirth", distribution=scipy.stats.uniform, min_year=0,
                                               max_year=0, date_format="%d/%m/%Y")
        dob_value = headfake.field.LookupField(field="DateOfBirth", hidden=True)  # Used in following functions
        gestational_age = BirthData.generate_gestational_age_old(field_name="GestationalAge", min_value=25,
                                                                 max_value=42)
        is_premature = BirthData.is_premature(field_name="Premature", gestational_age_field_name="GestationalAge")
        weight = BirthData.generate_weight(field_name="Weight", gestational_age=gestational_age)
        antibiotics_baby = BirthData.generate_antibiotics_baby("AntibioticsBaby",
                                                               options=THREE_VALUE_OPTIONS)
        antibiotics_mother = BirthData.generate_antibiotics_mother("AntibioticsMother",
                                                                   options=THREE_VALUE_OPTIONS)
        meconium = BirthData.has_meconium(field_name="Meconium", options=THREE_VALUE_OPTIONS)
        cortisone_baby = BirthData.generate_cortisone_baby("CortisoneBaby", options=THREE_VALUE_OPTIONS)
        cortisone_mother = BirthData.generate_cortisone_mother("CortisoneMother",
                                                               options=THREE_VALUE_OPTIONS)
        tyroyd_mother = BirthData.has_tyroid_mother("TyroidMother", options=THREE_VALUE_OPTIONS)
        baby_fed = BirthData.generate_baby_fed(field_name="BabyFed", options=THREE_VALUE_OPTIONS)
        hu_fed = BirthData.generate_hu_fed(field_name="HUFed", is_fed_field_name="BabyFed",
                                           options=THREE_VALUE_OPTIONS)
        mix_fed = BirthData.generate_mix_fed(field_name="MIXFed", is_fed_field_name="BabyFed",
                                             options=THREE_VALUE_OPTIONS)

        # Sample data
        sample_id = MetabolicTest.generate_sample_id(field_name="SampleBarcode", min_value=1, length=8)
        sampling_type = headfake.field.ConstantField(name="Sampling", value=None)  # TODO: Unknown allowed values
        sample_quality = MetabolicTest.generate_sample_quality(field_name="SampleQuality",
                                                               options=SAMPLE_QUALITY)
        sample_collected = MetabolicTest.generate_sample_date(field_name="SamTimeCollected", start_date=dob_value)
        sample_received = MetabolicTest.generate_sample_date(field_name="SamTimeReceived", start_date=sample_collected)
        too_young = MetabolicTest.too_young_sample(field_name="TooYoung", date_of_birth=dob,
                                                   date_sample_collected=sample_collected)

        # ART fed
        art_fed = headfake.field.OptionValueField(name="ARTFed", probabilities=THREE_VALUE_OPTIONS)

        # TPN fed
        tpn_fed = headfake.field.OptionValueField(name="TPNFed", probabilities=THREE_VALUE_OPTIONS)

        # EN fed
        en_fed = headfake.field.OptionValueField(name="ENTFed", probabilities=THREE_VALUE_OPTIONS)

        true_value = headfake.field.OptionValueField(probabilities=THREE_VALUE_OPTIONS)
        false_value = headfake.field.OptionValueField(probabilities={"0": 0.5, "NA": 0.5})

        # TPN CARN Fed
        condition = headfake.field.Condition(field="ENTFed", operator=operator.eq, value="1")
        tpn_carn_fed = headfake.field.IfElseField(name="TPNCARNFed", condition=condition, true_value=true_value,
                                                  false_value=false_value)

        # TPN MCT Fed
        tpn_mct_fed = headfake.field.IfElseField(name="TPNMCTFed", condition=condition, true_value=true_value,
                                                 false_value=false_value)

        # Hospital of referral
        hospital = headfake.field.OperationField(name="Hospital", operator=None, first_value="Italy", second_value=None,
                                                 operator_fn=Patient.generate_hospital_name_by_country)

        # Answer IX

        answer_ix = headfake.field.OptionValueField(name="AnswerIX", probabilities=ANSWER_IX)

        # Birth method
        birth_method = headfake.field.OptionValueField(name="BirthMethod", probabilities=BIRTH_METHOD)

        # Is BIS
        bis = headfake.field.OptionValueField(name="BIS", probabilities=THREE_VALUE_OPTIONS)

        # Twins
        twins = headfake.field.OptionValueField(name="Twins", probabilities=TWO_VALUE_OPTIONS)

        # Patient ID
        idGenerator = headfake.field.IncrementIdGenerator(min_value=1, length=8)
        pid = headfake.field.IdField(name="id", generator=idGenerator, prefix="BUZZI_")

        # Create fieldset
        fs = headfake.Fieldset(fields=[asaTotal, ala, allele1, allele2, arg, cit, glu, gly, leu_ile_pro_oh, met, orn,
                                       phe, pro, tyr, val, btd, c0, c2, c3, c3dc_c4oh, c4, c4dc_c5oh, c5, c5_1,
                                       c5dc_c6oh,
                                       c6, c6dc, c8, c8_1, c10, c10_1, c10_2, c12, c12_1, c14, c14_1, c14_2, c14_oh,
                                       c16,
                                       c16_1, c16_oh, c16_1_oh, c18, c18_1, c18_2, c18_oh, c18_1_oh, c18_2oh, c20, c22,
                                       c24, c26, sa, ado, d_ado, c20_0_lpc, c22_0_lpc, c24_0_lpc, c26_0_lpc, s_tsh,
                                       irt_gsp,
                                       tgal, mma, ema, ga, _2oh_ga, _3oh_ga, hcys, _3oh_pa, mca, orotico, piva, _2mbc,
                                       c4_b, c4_i,
                                       dob, sex, city, sampling_type, gestational_age, ethnicity, sample_id,
                                       sample_quality,
                                       sample_collected, sample_received, weight, antibiotics_baby, antibiotics_mother,
                                       meconium,
                                       cortisone_baby, cortisone_mother, tyroyd_mother, too_young, is_premature,
                                       too_young,
                                       baby_fed, hu_fed, mix_fed, art_fed, tpn_fed, en_fed, tpn_carn_fed, tpn_mct_fed,
                                       hospital, answer_ix, birth_method, bis, twins, pid])

        return fs

    def generate_diagnosis_dataset(self, id_list):
        column_1 = id_list
        column_2 = []

        buzzi_diseases = pd.read_csv(get_ground_data("BUZZI_Diseases.csv"), sep="\t")

        for _ in id_list:
            column_2.append(random.choice(buzzi_diseases["disease_name"]))

        data = {'id': column_1, 'diagnosis': column_2}
        df = pd.DataFrame(data=data)

        return df
