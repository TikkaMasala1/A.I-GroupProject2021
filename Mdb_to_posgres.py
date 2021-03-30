from pymongo import MongoClient
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import pprint

from time import time

postgresConnection = psycopg2.connect(user="postgres",
                                      password="root",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="huwebshop")
postgresConnection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = postgresConnection.cursor()

client = MongoClient("mongodb://localhost:27017/")
db = client["huwebshop"]
col = db["products"]


def get_products_mongo():
    products_array = []
    data_raw = col.find({'category': {"$ne": None}}, {'_id': 1, 'name': 1,
                                                      'category': 1, 'sub_category': 1, 'sub_sub_category': 1,
                                                      'price': {'selling_price': 1}
                                                      })
    for data in data_raw:
        products_array.append(data)
    return products_array


def create_table_products():
    cur.execute("""
        CREATE TABLE if not exists products (product_id varchar PRIMARY KEY, category varchar,
        sub_category varchar, sub_sub_category varchar,
        name varchar);
        """)
    print("Table created successfully")


create_table_products()
products = get_products_mongo()


cur.executemany("""INSERT INTO products(product_id,category,name)
VALUES (%(_id)s,%(category)s,%(name)s)""", products)

