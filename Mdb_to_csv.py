import pymongo
import pandas as pd
from bson.json_util import dumps

# initialiseert de databaseverbinding
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["shopping-minds"]


def products():
    mycol = mydb["products"]
    cursor = mycol.find()

    list_cur = list(cursor)
    json_data = dumps(list_cur, indent=2)

    with open('products.json', 'w+') as a:
        a.write(json_data)

    data_df = pd.read_json('products.json')
    data_df.to_csv('products.csv', index=None)


def sessions():
    mycol = mydb["sessions"]
    cursor = mycol.find()

    list_cur = list(cursor)
    json_data = dumps(list_cur, indent=2)

    with open('sessions.json', 'w+') as a:
        a.write(json_data)

    data_df = pd.read_json('sessions.json')
    data_df.to_csv('sessions.csv', index=None)


def visitors():
    mycol = mydb["visitors"]
    cursor = mycol.find()

    list_cur = list(cursor)
    json_data = dumps(list_cur, indent=2)

    with open('visitors.json', 'w+') as a:
        a.write(json_data)

    data_df = pd.read_json('visitors.json')
    data_df.to_csv('visitors.csv', index=None)


def data_to_csv():
    products()
    # sessions()
    # visitors()


data_to_csv()
