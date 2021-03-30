import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

'''Verbinden van pymongo en psycopg2 met python'''
postgresConnection = psycopg2.connect(user="postgres",
                                      password="root",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="huwebshop")
postgresConnection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = postgresConnection.cursor()
print("Opened database successfully")
'''inserting .csv file into database'''


def truncate():
    cursor.execute('TRUNCATE TABLE products;')
    cursor.execute('TRUNCATE TABLE brand;')
    cursor.execute('TRUNCATE TABLE category;')
    cursor.execute('TRUNCATE TABLE profiles;')
    cursor.execute('TRUNCATE TABLE sessions;')
    cursor.execute('TRUNCATE TABLE sub_category;')
    cursor.execute('TRUNCATE TABLE sub_sub_category;')
    cursor.execute('TRUNCATE TABLE orders;')
    cursor.execute('TRUNCATE TABLE temp;')


def prod_table():
    with open("products.csv", 'r') as f:
        next(f)
        cursor.copy_from(f, 'products', sep=';')
    print("products imported succesfully")


def sessions():
    with open("sessions.csv", 'r') as f:
        next(f)
        cursor.copy_from(f, 'sessions', sep=';')
    print("sessions imported succesfully")


def sessions_duplicate_delete():
    cursor.execute("""
        DELETE FROM sessions
        WHERE sessions_id IN
            (SELECT sessions_id
            FROM
                (SELECT sessions_id,
                ROW_NUMBER() OVER( PARTITION BY sessions
                ORDER BY sessions_id ) AS row_num
                FROM sessions ) t
                WHERE t.row_num >1);
        """)


def brand():
    cursor.execute("""
        INSERT INTO brand (brand)
        SELECT brand FROM products;
        """)
    print("brands imported succesfully")


def brand_duplicate_delete():
    cursor.execute("""
        DELETE FROM brand
        WHERE brand_id IN
            (SELECT brand_id
            FROM
                (SELECT brand_id,
                ROW_NUMBER() OVER( PARTITION BY brand
                ORDER BY brand_id ) AS row_num
                FROM brand ) t
                WHERE t.row_num >1);
        """)


def category():
    cursor.execute("""
        INSERT INTO category (category)
        SELECT category FROM products;
        """)
    print("categories imported succesfully")


def category_duplicate_delete():
    cursor.execute("""
        DELETE FROM category
        WHERE category_id IN
            (SELECT category_id
            FROM
                (SELECT category_id,
                ROW_NUMBER() OVER( PARTITION BY category
                ORDER BY category_id ) AS row_num
                FROM category ) t
                WHERE t.row_num >1);
        """)


def sub_category():
    cursor.execute("""
        INSERT INTO sub_category (sub_category)
        SELECT sub_category FROM products;
        """)
    print("sub categories imported succesfully")


def ins_sub_cat():
    '''ids van categoriein de tabel sub categorie toevoegen'''
    cursor.execute("""
        INSERT INTO sub_category(category_id)
        SELECT category_id FROM category;
        """)


def sub_sub_category():
    cursor.execute("""
        INSERT INTO sub_sub_category (sub_sub_category)
        SELECT sub_sub_category FROM products;
        """)
    print("sub sub categories imported succesfully")


def ins_sub_sub_cat():
    '''ids van categorie en subcategorie in de tabel sub sub categorie toevoegen'''
    cursor.execute("""
        INSERT INTO sub_sub_category (category_id, sub_category_id)
        SELECT category_id, sub_category_id FROM sub_category;
        """)


def prof_table():
    with open("profiles.csv", 'r') as f:
        next(f)
        cursor.copy_from(f, 'profiles', sep=';')
    print("profiles imported succesfully")


def order():
    cursor.execute("""
        INSERT INTO orders(product_id)
        SELECT product_id FROM products
        """)


def temp():
    cursor.execute("""
      INSERT INTO temp(orders, number)
        SELECT bestelling, COUNT(bestelling) AS value_occurrence 
        FROM sessions
        WHERE has_sale = 'True'
        GROUP BY bestelling
        ORDER BY value_occurrence DESC
        LIMIT 50;
    """)


def table_insert():
    truncate()
    prod_table()
    brand()
    category()
    prof_table()
    sessions()
    sub_category()
    sub_sub_category()
    ins_sub_cat()
    ins_sub_sub_cat()
    order()
    temp()


table_insert()

postgresConnection.commit()
postgresConnection.close()
print('files loaded into database')
postgresConnection.close()
