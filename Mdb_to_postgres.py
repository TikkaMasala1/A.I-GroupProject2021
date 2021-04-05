from pymongo import MongoClient
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
def data_transfer_products():
    print("Data transfer products started")
    cur.executemany("""INSERT INTO PRODUCTS(product_id,product_name,gender,category,sub_category,sub_sub_category,price)
    VALUES (%(_id)s,%(name)s,%(gender)s,%(category)s,%(sub_category)s,%(sub_sub_category)s,%(price)s)""", products_data)
    print("Data transfer products successful\n")


# Pakt de id's van de verkochte products en zet die om in een dictionary
def get_sessions_mongo():
    print("Sessions data retrieval started")
    col = db["sessions"]
    order_array = []
    data_raw = col.find({'has_sale': {"$ne": False}}, {'_id': 1, 'order': {'products': {'id': 1}}})

    # Dit gaat door de raw data heen en plukt alleen de benodigde id's eruit.
    for data in data_raw:
        if 'order' not in data:
            continue
        if data['order']['products'] is None:
            continue
        for x in data['order']['products']:
            order_array.append(x)
    print("Sessions data retrieval successful\n")
    return order_array


# Verwijderd de sessions table, dit is voor testing doeleinde
def delete_table_sessions():
    cur.execute("""
        DROP TABLE if exists SESSIONS;    
    """)
    print("Session table deleted successfully")


# Maakt de sessions tabel aan
def create_table_sessions():
    cur.execute("""
        CREATE TABLE if not exists SESSIONS (session_id serial PRIMARY KEY, product_id varchar);
        """)
    print("Session table created successfully\n")


# Insert de list van dictionary's, die de id's bevatten in postgres
def data_transfer_sessions():
    print("Data transfer sessions starting")
    cur.executemany("""INSERT INTO SESSIONS(product_id)
    VALUES (%(id)s)""", orders)
    print("Data transfer sessions successful")


if __name__ == '__main__':
    products_data = get_products_mongo()
    orders = get_sessions_mongo()
    delete_table_products()
    create_table_products()
    data_transfer_products()
    delete_table_sessions()
    create_table_sessions()
    data_transfer_sessions()
    cur.close()
    postgresConnection.close()
