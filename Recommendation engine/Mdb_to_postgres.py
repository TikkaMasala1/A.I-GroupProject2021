from pymongo import MongoClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from collections import Counter
from pandas.io.json._normalize import nested_to_record

import psycopg2
import json

# Intialiseert de databaseverbinding met Postgres
postgresConnection = psycopg2.connect(user="postgres",
                                      password="groep6",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="huwebshop")
postgresConnection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = postgresConnection.cursor()

# Intialiseert de databaseverbinding met MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["huwebshop"]


# Zet de products van de mongo database om in een list van dictonary's
def get_products_mongo():
    print("Products data retrieval started")
    col = db["products"]
    products_array = []
    data_raw = col.find({'category': {"$ne": None}}, {'_id': 1, 'name': 1,
                                                      'category': 1, 'sub_category': 1, 'sub_sub_category': 1,
                                                      'price': {'discount': 1, 'mrsp': 1, 'selling_price': 1},
                                                      'gender': 1
                                                      })
    # Dit verplat de data
    data_raw_flat = nested_to_record(data_raw, sep='_')
    for data in data_raw_flat:
        # Sommige "sub_category", "sub_sub_category", "price_mrsp" en "price_selling_price"
        # velden bevatten gegevens die fouten veroorzaken bij het invoegen in Postgres.
        # Dit filtert de gegevens en vervangt ze met "None"
        if 'sub_category' not in data:
            data['sub_category'] = None
        if 'sub_sub_category' not in data:
            data['sub_sub_category'] = None
        if 'price_mrsp' not in data:
            data['price_mrsp'] = None
            data['price_discount'] = None
        if 'price_selling_price' not in data:
            data['price_selling_price'] = None
            data['price_discount'] = None
        if data['price_selling_price'] == data['price_mrsp']:
            data['price_discount'] = None
        if data['price_discount'] is not None:
            data['price_discount'] = round(
                (((data['price_mrsp'] - data['price_selling_price']) / data['price_mrsp']) * 100))
        products_array.append(data)
    print("Products data retrieval successful\n")
    return products_array


# Verwijderd de products table, dit is voor testing doeleinde
def delete_table_products():
    cur.execute("""
        DROP TABLE if exists PRODUCTS;    
    """)
    print("Products table deleted successfully")


# Maakt de products tabel aan
def create_table_products():
    cur.execute("""
        CREATE TABLE if not exists PRODUCTS (product_id varchar PRIMARY KEY, product_name varchar, mrsp int,
         selling_price int, discount int , gender varchar, category varchar, sub_category varchar, sub_sub_category varchar);
        """)
    print("Products table created successfully\n")


# Insert de list van dictionary's in de postgres database
def data_transfer_products(data):
    print("Data transfer products started")
    cur.executemany("""INSERT INTO PRODUCTS(product_id, product_name, mrsp, selling_price, discount, gender, category,
    sub_category, sub_sub_category)
     VALUES (%(_id)s,%(name)s,%(price_mrsp)s,%(price_selling_price)s,%(price_discount)s,%(gender)s,%(category)s,%(sub_category)s,
     %(sub_sub_category)s)""", data)
    print("Data transfer products successful\n")


def get_sessions_mongo():
    print("Sessions data retrieval started")
    col = db["sessions"]
    session_array = []
    data_raw = col.find({'has_sale': {"$ne": False}}, {'_id': 1, 'buid': 1, 'order': 1, 'segment': 1})

    # Dit gaat door de raw data heen en format het voor postgres.
    for data in data_raw:
        if 'buid' in data and 'order' in data:
            if not data['buid'] or not data['order']:
                continue
            if 'segment' not in data:
                data['segment'] = 'None'
            data['_id'] = str(data['_id'])
            data['buid'] = data['buid'][0]
            data['order'] = [f['id'] for f in data['order']['products']]
            session_array.append(data)
    print("Sessions data retrieval successful\n")
    return session_array


# Verwijderd de sessions table, dit is voor testing doeleinde
def delete_table_sessions():
    cur.execute("""
        DROP TABLE if exists SESSIONS;    
    """)
    print("Session table deleted successfully")


# Maakt de sessions tabel aan
def create_table_sessions():
    cur.execute("""
        CREATE TABLE if not exists SESSIONS (session_id varchar PRIMARY KEY, buid varchar,product_id varchar,
        segment varchar);
        """)
    print("Session table created successfully\n")


# Insert de list van dictionary's, die de id's bevatten in postgres
def data_transfer_sessions(data):
    print("Data transfer sessions starting")
    cur.executemany("""INSERT INTO SESSIONS(session_id, buid, product_id, segment)
    VALUES (%(_id)s, %(buid)s, %(order)s, %(segment)s)""", data)
    print("Data transfer sessions successful\n")


def get_profiles_mongo():
    print("Profiles data retrieval started")
    col = db["profiles"]
    profile_array = []
    data_raw = col.find({'has_sale': {"$ne": False}}, {'_id': 1, 'buids': 1, 'recommendations.similars': 1})

    # Dit gaat door de raw data heen en format het voor postgres.
    for data in data_raw:
        if 'buids' not in data or 'recommendations' not in data or data['buids'] is None \
                or not data['recommendations']['similars']:
            continue

        data['recommendations'] = data['recommendations']['similars']
        data['_id'] = str(data['_id'])
        profile_array.append(data)
    print("Profiles data retrieval successful\n")
    return profile_array


# Verwijderd de profiles table, dit is voor testing doeleinde
def delete_table_profiles():
    cur.execute("""
        DROP TABLE if exists PROFILES;    
    """)
    print("Profile table deleted successfully")


# Maakt de profiles tabel aan
def create_table_profiles():
    cur.execute("""
        CREATE TABLE if not exists PROFILES (profile_id varchar PRIMARY KEY, buids varchar, similars varchar);
        """)
    print("Profile table created successfully\n")


# Insert de list van dictionary's in postgres
def data_transfer_profiles(data):
    print("Data transfer profiles started")
    cur.executemany("""INSERT INTO PROFILES(profile_id,buids,similars)
    VALUES (%(_id)s,%(buids)s, %(recommendations)s)""", data)
    print("Data transfer profiles successful\n")


def get_pop_products_mongo():
    print("Pop_products data retrieval started")
    cur.execute('SELECT product_id FROM sessions')
    data = cur.fetchall()
    counter_data = Counter(elem[0] for elem in data)

    lst = []
    for key in counter_data.keys():
        key = key.replace('{', '')
        key = key.replace('}', '')
        key = key.split(',')
        lst += key

    sol = dict()
    for _id in lst:
        if _id in sol.keys():
            sol[_id] += 1
        else:
            sol[_id] = 1

    sorted_x = sorted(sol.items(), key=lambda kv: kv[1], reverse=True)
    print("Pop_products data retrieval successful\n")
    return sorted_x


def delete_table_pop_products():
    cur.execute("""
        DROP TABLE if exists POP_PRODUCTS;
    """)
    print("Pop_products table deleted successfully")


# Maakt de profiles tabel aan
def create_table_pop_products():
    cur.execute("""
        CREATE TABLE if not exists POP_PRODUCTS (product_id varchar PRIMARY KEY, freq int, nodup varchar);
        """)
    print("Pop_products table created successfully\n")


# Insert de list van dictionary's in postgres
def data_transfer_pop_products(data):
    print("Data transfer pop_products started")
    cur.executemany("""INSERT INTO POP_PRODUCTS(product_id,freq)
    VALUES (%s, %s)""", data)
    print("Data transfer pop_products successful")


if __name__ == '__main__':
    product_data = get_products_mongo()
    session_data = get_sessions_mongo()
    profile_data = get_profiles_mongo()
    pop_products_data = get_pop_products_mongo()

    delete_table_products()
    create_table_products()
    data_transfer_products(product_data)

    delete_table_sessions()
    create_table_sessions()
    data_transfer_sessions(session_data)

    delete_table_profiles()
    create_table_profiles()
    data_transfer_profiles(profile_data)

    delete_table_pop_products()
    create_table_pop_products()
    data_transfer_pop_products(pop_products_data)
    cur.close()
    postgresConnection.close()