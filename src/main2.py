import inflection

import bson
from dateutil.parser import parse
from pymongo.mongo_client import MongoClient
from bson.json_util import loads

from utils.utils import get_mongodb_date_from_datetime, normalize_column_value, read_csv_file_as_string


def to_snake_case(name):
    return inflection.underscore(name)
    # pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    # return pattern.sub('_', name).lower()
    # name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # name = re.sub('__([A-Z])', r'_\1', name)
    # name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    # return name.lower()


def load_json_from_file_as_bson():
    # d1 = datetime.datetime.strptime("2018-10-13T11:56:52.000Z", "%Y-%m-%dT%H:%M:%S.000Z")
    # d2 = datetime.datetime.strptime("2017-10-13T10:53:53.000Z", "%Y-%m-%dT%H:%M:%S.000Z")
    # d3 = datetime.datetime.strptime("2022-09-13T10:53:53.000Z", "%Y-%m-%dT%H:%M:%S.000Z")
    with MongoClient() as mongo:
        db = mongo.get_database("mydb")
        # my_tuple1 = { "mydate": { "$date": d1.isoformat() } }
        # my_tuple2 = { "mydate": { "$date": d2.isoformat() } }
        # my_tuple3 = { "mydate": { "$date": d3.isoformat() } }
        # { $dateFromString: {
        #     dateString: "2017-02-08T12:10:40.787"
        # } }
        # my_tuples = [my_tuple1, my_tuple2, my_tuple3]
        # with open("../datasets/test_dates/data.json", "w") as data_file:
        #   json.dump(my_tuples, data_file)
        with open("../datasets/test_dates/data.json", "r") as data_file:
            # my_tuples = json.load(data_file)
            my_tuples = bson.json_util.loads(data_file.read())
            # for my_tuple in my_tuples:
            #     db["mycollectionIII"].insert_one(my_tuple)
            db["mycollectionIX"].insert_many(my_tuples, ordered=False)


def change_int_in_fct(my_int: int) -> None:
    my_int = my_int + 1


def change_tab_in_fct(my_tab: list[str]):
    my_tab.append("another string")


def change_tab_with_objects_in_fct(my_tab: list[dict]):
    my_tab.append({"key": "another object"})


def compare_unordered_list(list_a: list[str], list_b: list[str]) -> bool:
    return list_a == list_b


def convert_to_datetime(my_value: str):
    try:
        datetime_value = parse(my_value)
        # %Y-%m-%d %H:%M:%S is the format used by default by parse (the output is always of this form)
        return get_mongodb_date_from_datetime(datetime_value)
    except ValueError:
        # this was not a datetime value, and we signal it with None
        return None


if __name__ == '__main__':
    # load_json_from_file_as_bson()

    # a = 1
    # print(a)
    # change_int_in_fct(a)
    # print(a)
    #
    # b = ["a string"]
    # print(b)
    # change_tab_in_fct(b)
    # print(b)
    #
    # c = [{"key": "an object"}]
    # print(c)
    # change_tab_with_objects_in_fct(c)
    # print(c)

    # list_a = ["a", "b", "c"]
    # list_b = ["c", "a", "b"]
    # list_c = ["a", "b", "c"]
    # list_d = copy.deepcopy(list_b)
    # list_d.sort()
    # print(compare_unordered_list(list_a, list_b))
    # print(compare_unordered_list(list_a, list_c))
    # print(compare_unordered_list(list_a, list_d))

    # print(to_snake_case("nelly"))
    # print(to_snake_case("Nelly"))
    # print(to_snake_case("NELly"))
    # print(to_snake_case("theNELly"))
    # print(to_snake_case("NELLY"))
    # print(to_snake_case("getHTTPResponseCode"))
    # print(to_snake_case("molecule_a"))
    # print(to_snake_case("Molecule_a"))
    # print(to_snake_case("molecule_A"))
    # print(to_snake_case("Molecule_A"))
    # print(to_snake_case(" Molecule_A"))
    # print(to_snake_case(" Nelly "))
    # print(to_snake_case(" Nel ly "))
    # print(normalize_hospital_name(HospitalNames.IT_BUZZI_UC1.value))

    # print(convert_to_datetime("abc"))
    # print(convert_to_datetime("white"))
    # print(convert_to_datetime("2024-03-08"))
    # print(convert_to_datetime("2024-03-08 15:45:00"))
    # print(convert_to_datetime("2024-03-08 15:45:00.245"))

    # print({"a": 2, "b": 1} == {"b": 1, "a": 2})

    data = read_csv_file_as_string("../datasets/my_data.csv")
    print(data)

    print("#####")
    for index, row in data.iterrows():
        print(f"row: \n{row.to_string()}")
        for i_col in range(len(data.columns)):
            column = data.columns[i_col]
            print(f"line {index}, column {column}: value is {row.iloc[i_col]} and is of type {type(row.iloc[i_col])}")
        print()

    for column in data:
        data[column] = data[column].apply(lambda x: normalize_column_value(column_value=x))

    print("#####")
    for index, row in data.iterrows():
        print(f"row: \n{row.to_string()}")
        for i_col in range(len(data.columns)):
            column = data.columns[i_col]
            print(f"line {index}, column {column}: value is {row.iloc[i_col]} and is of type {type(row.iloc[i_col])}")
        print()

    print("Done.")
