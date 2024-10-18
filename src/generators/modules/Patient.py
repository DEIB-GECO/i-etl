import datetime
import operator
import random

import headfake
import headfake.field
import headfake.transformer
import pandas as pd
from faker import Faker

from utils.file_utils import get_ground_data

df_genes_to_phenotype = pd.read_csv(get_ground_data("genes_to_phenotype.txt"), sep="\t")
df_countries = pd.read_csv(get_ground_data("countries.csv"), sep=",")
df_diseases = pd.read_csv(get_ground_data("IMGGE_Diseases.csv"), sep=",")


class Patient:
    __patientID = None
    __clinicalCenterv = None
    __sex = None
    __ageWhenTested = None
    __ageOnset = None

    def __init__(self):
        pass

    @staticmethod
    # TODO: Refine hospital names
    def generate_hospital_name_by_country(country, *arguments):
        x = df_countries.loc[df_countries['Name'] == country]

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

    @staticmethod
    def generate_sex(field_name, male_value, female_value):
        sex = headfake.field.derived.GenderField(name=field_name, male_value=male_value, female_value=female_value)
        return sex

    @staticmethod
    def generate_patient_id(field_name, min_value, length, prefix):
        idGenerator = headfake.field.IncrementIdGenerator(min_value=min_value, length=length)
        pid = headfake.field.IdField(name=field_name, generator=idGenerator, prefix=prefix)
        return pid


class Age:
    years = None
    months = None

    # Creates an Age object with the values provided by the years and months parameters or creates an Age object with 
    # random values between 0 and 100 years, and 0 and 11 months
    def __init__(self, years, months):
        if years is not None:
            self.years = years
        else:
            self.years = random.randint(a=0, b=100)

        if months is not None:
            self.months = months
        else:
            self.months = random.randint(a=0, b=11)

    def __str__(self):
        if self.years == 0:
            return f"{self.months}m"
        elif self.months == 0:
            return f"{self.years}y"
        else:
            return f"{self.years}y, {self.months}m"

    # Returns the difference between the current month and the month of birth
    @staticmethod
    def get_months_difference(date_of_birth=str, current_date=datetime):
        first_month = datetime.datetime.strptime(date_of_birth, "%d.%m.%Y").month
        second_month = current_date.month
        if second_month >= first_month:
            return second_month - first_month
        else:
            return 0

    # Returns age and months in years
    @staticmethod
    def get_age_summary(years=int, months=int):
        return round((years + months) / 12, 2)

    # Returns age as a formated string
    @staticmethod
    def age_to_string(years=int, months=int):
        if years == 0:
            return f"{months}m"
        elif months == 0:
            return f"{years}y"
        else:
            return f"{years}y, {months}m"


class BirthData:
    __weigthAtBirth: None
    __lengthAtBirth: None
    __OFCAtBirth: None
    __dateOfBirth: None
    __gestationalAge: None
    __antibioticsBaby: None
    __antibioticsMother: None
    __meconium: None
    __cortisoneBaby: None
    __cortisoneMother: None
    __tyroidMother: None
    __answerIX: None
    __bithMethod: None
    __twins: None
    __hasSpontaneousAbortions: None
    __numberOfAbortions: None
    __inVitroFertilization: None

    def __init__(self):
        pass

    @staticmethod
    def __gestational_age_to_string(gestational_age, *arguments):
        return f'{gestational_age} setm.'

    @staticmethod
    def get_gestational_age(min_value, max_value):
        return random.randint(a=min_value, b=max_value)

    @staticmethod
    def generate_gestational_age(min_value, max_value):
        return BirthData.get_gestational_age(min_value, max_value)

    @staticmethod
    def generate_gestational_age_old(field_name, min_value, max_value):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=min_value,
                                             second_value=max_value, operator_fn=BirthData.get_gestational_age)

    @staticmethod
    def gestational_age_to_string(field_name, ga):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=ga, second_value=None,
                                             operator_fn=BirthData.__gestational_age_to_string)

    @staticmethod
    def get_weight_at_birth(is_premature=bool, *arguments):
        premature = None

        if isinstance(is_premature, str):
            premature = bool(is_premature)

        if premature:
            return f'{round(random.randint(a=1500, b=2500) / 1000, 1)} kg'
        else:
            return f'{round(random.randint(a=2500, b=4000) / 1000, 1)} kg'

    @staticmethod
    def get_length_at_birth(is_premature=bool, *arguments):
        premature = None

        if isinstance(is_premature, str):
            premature = bool(is_premature)

        if premature:
            return f'{round(random.uniform(a=25.5, b=45.4), 2)} cm'
        else:
            return f'{round(random.uniform(a=45.5, b=53.4), 2)} cm'

    @staticmethod
    def get_ofc_at_birth(is_premature=bool, *arguments):
        premature = None

        if isinstance(is_premature, str):
            premature = bool(is_premature)

        if premature:
            return f'{round(random.uniform(a=24, b=33.5), 2)} cm'
        else:
            return f'{round(random.uniform(a=34, b=36), 2)} cm'

    @staticmethod
    def generate_date_of_birth(field_name, distribution, min_year, max_year, date_format):
        dob = headfake.field.DateOfBirthField(name=field_name, distribution=distribution, mean=min_year, sd=max_year,
                                              min=0, max=0, date_format=date_format)
        return dob

    @staticmethod
    def is_premature(field_name, gestational_age_field_name):
        condition = headfake.field.Condition(operator=operator.lt, field=gestational_age_field_name, value=37)
        is_premature = headfake.field.IfElseField(name=field_name, condition=condition, true_value="1", false_value="0")
        return is_premature

    @staticmethod
    def generate_weight(field_name, gestational_age):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=gestational_age,
                                             second_value=None, operator_fn=BirthData.get_weight_at_birth)

    @staticmethod
    # TODO: Truncate to 2 decimals
    def generate_length(field_name, gestational_age):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=gestational_age,
                                             second_value=None, operator_fn=BirthData.get_length_at_birth)

    @staticmethod
    # TODO: Truncate to 2 decimals
    def generate_ofc(field_name, gestational_age):
        return headfake.field.OperationField(name=field_name, operator=None, first_value=gestational_age,
                                             second_value=None, operator_fn=BirthData.get_ofc_at_birth)

    @staticmethod
    def generate_antibiotics_baby(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def generate_antibiotics_mother(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def has_meconium(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def generate_cortisone_baby(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def generate_cortisone_mother(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def has_tyroid_mother(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def generate_baby_fed(field_name, options):
        return headfake.field.OptionValueField(name=field_name, probabilities=options)

    @staticmethod
    def generate_hu_fed(field_name, is_fed_field_name, options):
        condition = headfake.field.Condition(field=is_fed_field_name, operator=operator.eq, value="1")
        true_value = headfake.field.OptionValueField(probabilities=options)
        false_value = headfake.field.OptionValueField(probabilities={"0": 0.5, "NA": 0.5})
        return headfake.field.IfElseField(name=field_name, condition=condition, true_value=true_value,
                                          false_value=false_value)

    @staticmethod
    def generate_mix_fed(field_name, is_fed_field_name, options):
        condition = headfake.field.Condition(field=is_fed_field_name, operator=operator.ne, value="1")
        true_value = headfake.field.OptionValueField(probabilities=options)
        false_value = headfake.field.OptionValueField(probabilities={"0": 0.5, "NA": 0.5})
        return headfake.field.IfElseField(name=field_name, condition=condition, true_value=true_value,
                                          false_value=false_value)


class GeographicData:
    city = None
    ethnicity = None
    country = None

    # Initiates a GeographicData object with realistic and coherent data
    def __init__(self, city, ethnicity, country):
        if country != None:
            self.country = country
        else:
            # TODO: Get country from a list
            pass

        if city is not None:
            self.city = city
        else:
            # TODO: Get country locale and fake a city with faker
            pass

        if ethnicity is not None:
            self.ethnicity = ethnicity
        else:
            # TODO: Select ethnicity from a list based on country
            pass

    @staticmethod
    # Generates a city name based on locale configuration (e.g. es_ES)
    def generate_city(field_name, locale):
        def generate_city_name(locale, *arguments):
            fake = Faker(locale)
            return fake.city()

        return headfake.field.OperationField(name=field_name, operator=None, first_value=locale, second_value=None,
                                             operator_fn=generate_city_name)

    @staticmethod
    # Selects a random ethnicity from a list of ethnicites with defined probabilities. If no ethnicities list is provided, the function uses
    # an internal set of ethnicities.
    def generate_ethnicity(field_name, ethnicities):
        if ethnicities is None:
            ETHNICITIES = {"African": 0.1, "East asian": 0.1, "Middle eastern": 0.2, "European": 0.3,
                           "Latin American": 0.2, "Jewish": 0.1}
            return headfake.field.OptionValueField(name=field_name, probabilities=ETHNICITIES)
        else:
            return headfake.field.OptionValueField(name=field_name, probabilities=ethnicities)

    @staticmethod
    # Selects a random country from an internal list of european countries
    def generate_country_of_birth(field_name, ):
        return headfake.field.MapFileField(name=field_name, mapping_file=get_ground_data("countries.csv"), key_field="Name")
