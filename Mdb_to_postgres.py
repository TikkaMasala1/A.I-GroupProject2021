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


# Zet de gegevens van de mongo database om in een list van dictonary's
def get_products_mongo():
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
    return products_array


# Verwijderd de table, dit is voor testing doeleinde
def delete_table_products():
    cur.execute("""
        DROP TABLE if exists PRODUCTS;    
    """)
    print("Product table deleted successfully")


# Maakt de products tabel aan
def create_table_products():
    cur.execute("""
        CREATE TABLE if not exists PRODUCTS (product_id varchar PRIMARY KEY, product_name varchar, price int, gender varchar,
        category varchar, sub_category varchar, sub_sub_category varchar);
        """)
    print("Product table created successfully")


# Insert de list van dictionary's in de postgres database
def data_transfer_products():
    cur.executemany("""INSERT INTO PRODUCTS(product_id,product_name,gender,category,sub_category,sub_sub_category,price)
    VALUES (%(_id)s,%(name)s,%(gender)s,%(category)s,%(sub_category)s,%(sub_sub_category)s,%(price)s)""", products_data)
    print("Data transfer successful")


products_data = get_products_mongo()
delete_table_products()
create_table_products()
data_transfer_products()
# data_transfer_sessions()
cur.close()
postgresConnection.close()
