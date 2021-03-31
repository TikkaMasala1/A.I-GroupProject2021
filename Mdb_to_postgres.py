from pymongo import MongoClient
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
from psycopg2.extras import execute_values

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
        if 'sub_category' not in data:
            data['sub_category'] = None
        if 'sub_sub_category' not in data:
            data['sub_sub_category'] = None
        products_array.append(data)
    return products_array


def delete_table_products():
    cur.execute("""
        DROP TABLE if exists PRODUCTS;    
    """)
    print("Table deleted successfully")


def create_table_products():
    cur.execute("""
        CREATE TABLE if not exists PRODUCTS (product_id varchar PRIMARY KEY, category varchar, product_name varchar,
        sub_category varchar , sub_sub_category varchar);
        """)
    print("Table created successfully")


delete_table_products()
create_table_products()
products_data = get_products_mongo()


cur.executemany("""INSERT INTO PRODUCTS(product_id,category,product_name,sub_category,sub_sub_category)
VALUES (%(_id)s,%(category)s,%(name)s,%(sub_category)s,%(sub_sub_category)s)""", products_data)
