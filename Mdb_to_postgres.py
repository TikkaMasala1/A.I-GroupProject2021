from pymongo import MongoClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
import json

# Intialiseert de databaseverbinding met Postgres
postgresConnection = psycopg2.connect(user="postgres",
                                      password="root",
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
                                                      'price': {'selling_price': 1}, 'gender': 1
                                                      })

    for data in data_raw:
        # Sommige "sub_category" en "sub_sub_category" velden bevatten gegevens die fouten veroorzaken bij het invoegen
        # in Postgres. Dit filtert de gegevens en vervangt ze met "None"
        if 'sub_category' not in data:
            data['sub_category'] = None
        if 'sub_sub_category' not in data:
            data['sub_sub_category'] = None

        # Het veld "price" is een nested dictionary, dit haalt de gegevens uit de nested dictionary.
        if 'price' in data:
            for (key, value) in data['price'].items():
                data['price'] = value
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
        CREATE TABLE if not exists PRODUCTS (product_id varchar PRIMARY KEY, product_name varchar, price int, gender varchar,
        category varchar, sub_category varchar, sub_sub_category varchar);
        """)
    print("Products table created successfully\n")


# Insert de list van dictionary's in de postgres database
def data_transfer_products(data):
    print("Data transfer products started")
    cur.executemany("""INSERT INTO PRODUCTS(product_id,product_name,gender,category,sub_category,sub_sub_category,price)
    VALUES (%(_id)s,%(name)s,%(gender)s,%(category)s,%(sub_category)s,%(sub_sub_category)s,%(price)s)""", data)
    print("Data transfer products successful\n")


def get_sessions_mongo():
    print("Sessions data retrieval started")
    col = db["sessions"]
    session_array = []
    data_raw = col.find({'has_sale': {"$ne": False}}, {'_id': 0, 'buid': 1, 'order': 1})

    # Dit gaat door de raw data heen en format het voor postgres.
    for data in data_raw:
        if 'buid' in data and 'order' in data:
            if not data['buid']:
                continue
            if not data['order']['products']:
                continue
            data['buid'] = data['buid'][0]
            data['order'] = data['order']['products']
            data['order'] = list(map(lambda x: json.dumps(x), data['order']))
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
        CREATE TABLE if not exists SESSIONS (session_id serial PRIMARY KEY, buid varchar,product_id varchar);
        """)
    print("Session table created successfully\n")


# Insert de list van dictionary's, die de id's bevatten in postgres
def data_transfer_sessions(data):
    print("Data transfer sessions starting")
    cur.executemany("""INSERT INTO SESSIONS(buid,product_id)
    VALUES (%(buid)s, %(order)s)""", data)
    print("Data transfer sessions successful\n")


def get_profiles_mongo():
    print("Profiles data retrieval started")
    col = db["profiles"]
    profile_array = []
    data_raw = col.find({'has_sale': {"$ne": False}}, {'_id': 0, 'buids': 1, 'recommendations.similars': 1})

    # Dit gaat door de raw data heen en format het voor postgres.
    for data in data_raw:
        if 'buids' not in data or 'recommendations' not in data:
            continue
        if data['buids'] is None:
            continue
        if not data['recommendations']['similars']:
            continue
        data['recommendations'] = data['recommendations']['similars']
        data['buids'] = list(map(lambda x: json.dumps(x), data['buids']))
        data['recommendations'] = list(map(lambda x: json.dumps(x), data['recommendations']))
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
        CREATE TABLE if not exists PROFILES (profile_id serial PRIMARY KEY, buids varchar, similars varchar);
        """)
    print("Profile table created successfully\n")


# Insert de list van dictionary's in postgres
def data_transfer_profiles(data):
    print("Data transfer profiles started")
    cur.executemany("""INSERT INTO PROFILES(buids,similars)
    VALUES (%(buids)s, %(recommendations)s)""", data)
    print("Data transfer profiles successful")


if __name__ == '__main__':
    product_data = get_products_mongo()
    session_data = get_sessions_mongo()
    profile_data = get_profiles_mongo()
    delete_table_products()
    create_table_products()
    data_transfer_products(product_data)
    delete_table_sessions()
    create_table_sessions()
    data_transfer_sessions(session_data)
    delete_table_profiles()
    create_table_profiles()
    data_transfer_profiles(profile_data)
    cur.close()
    postgresConnection.close()
