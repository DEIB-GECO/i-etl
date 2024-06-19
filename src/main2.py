import bson
from pymongo.mongo_client import MongoClient
import datetime
from bson.json_util import loads
import json

if __name__ == '__main__':
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
