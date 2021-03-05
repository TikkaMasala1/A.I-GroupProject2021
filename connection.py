import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

'''Verbinden van pymongo en psycopg2 met python'''
postgresConnection = psycopg2.connect(user="postgres",
                                      password="groep6",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="huwebshop")
postgresConnection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = postgresConnection.cursor()
print("Opened database successfully")

''' creeeren van tabellen & kolommen'''
def drop():
    cursor.execute('DROP TABLE IF EXISTS products;')
    cursor.execute('DROP TABLE IF EXISTS brand;')
    cursor.execute('DROP TABLE IF EXISTS category;')
    cursor.execute('DROP TABLE IF EXISTS sub_category;')
    cursor.execute('DROP TABLE IF EXISTS sub_sub_category;')
    cursor.execute('DROP TABLE IF EXISTS sessions;')
    cursor.execute('DROP TABLE IF EXISTS orders;')
    cursor.execute('DROP TABLE IF EXISTS buids;')
    cursor.execute('DROP TABLE IF EXISTS profiles;')
    cursor.execute('DROP TABLE IF EXISTS temp;')


def products():
    cursor.execute("""
        CREATE TABLE products (product_id varchar PRIMARY KEY, brand varchar, category varchar,
        sub_category varchar, sub_sub_category varchar, color varchar, gender varchar, repeat_buy varchar,
        name varchar, price real, recommendable varchar);
        """)
    print("Table created successfully")


def brand():
    cursor.execute("""
        CREATE TABLE brand (brand_id serial PRIMARY KEY, brand varchar);
        """)
    print("Table created successfully")


def category():
    cursor.execute("""
        CREATE TABLE category (category_id serial PRIMARY KEY, category varchar);
        """)
    print("Table created successfully")


def sub_category():
    cursor.execute("""
        CREATE TABLE sub_category (sub_category_id serial PRIMARY KEY, category_id varchar, sub_category varchar);
        """)
    print("Table created successfully")


def sub_sub_category():
    cursor.execute("""
        CREATE TABLE sub_sub_category (sub_sub_category_id serial PRIMARY KEY, sub_category_id varchar, category_id varchar, sub_sub_category varchar);
        """)
    print("Table created successfully")


def temp():
    cursor.execute("""
        CREATE TABLE temp (orders varchar, number int);
        """)
    print("Table created successfully")


def orders():
    cursor.execute("""
        CREATE TABLE orders (order_id serial PRIMARY KEY, session_id varchar, product_id varchar);
        """)
    print("Table created successfully")

def profiles():
    cursor.execute("""
        CREATE TABLE profiles (profile_id varchar PRIMARY KEY, buid_id varchar array, segment varchar, viewed_before varchar array, similars varchar array, previously_recommended varchar array);
        """)
    print("Table created successfully")

def sessions():
    cursor.execute("""
        CREATE TABLE sessions (session_id varchar PRIMARY KEY, buid_id varchar, session_start varchar, 
        session_end varchar, has_sale varchar, bestelling varchar, segment varchar, preference_category varchar,
        preference_sub_cat varchar, preference_sub_sub_cat varchar)
        """)
    print("Table created successfully")

def tabellen():
    drop()
    products()
    brand()
    category()
    sub_category()
    sub_sub_category()
    sessions()
    orders()
    profiles()
    temp()


tabellen()
postgresConnection.commit()
postgresConnection.close()
print("Operation done successfully")
postgresConnection.close()

