from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import psycopg2
import random

app = Flask(__name__)
api = Api(app)

postgresConnection = psycopg2.connect(user="postgres",
                                      password="groep6",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="huwebshop")
c = postgresConnection.cursor()

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER", "MONGODBPASSWORD", "MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop


class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    def get(self, profileid, count, recom_type, productid):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """

        prodids = []
        if recom_type == 0:
            # a few querys to get de productids from different columns
            queryc = "SELECT category FROM products WHERE product_id = '" + productid + "'"
            c.execute(queryc)
            category = c.fetchone()
            querysub = "SELECT sub_category FROM products WHERE product_id = '" + productid + "'"
            c.execute(querysub)
            subcategory = c.fetchone()
            querysubsub = "SELECT sub_sub_category FROM products WHERE product_id = '" + productid + "'"
            c.execute(querysubsub)
            subsubcategory = c.fetchone()
            queryg = "SELECT gender FROM products WHERE product_id = '" + productid + "'"
            c.execute(queryg)
            gender = c.fetchone()

            for i in range(0,int(count/2)):
                # in this if statement we choose different products from the same category but not the same product
                queryp = """SELECT product_id FROM products 
                                            WHERE category LIKE '""" + category[0] + """' 
                                            AND product_id NOT LIKE CAST(""" + productid + """ AS varchar)
                                            AND gender LIKE '""" + gender[0] + """' OR gender LIKE 'Unisex'
                                            ORDER BY random() LIMIT 1;"""
                c.execute(queryp)
                product = c.fetchone()
                prodids += product

            for j in range(0, int(count / 4)):
                # in this if statement we choose different products from the same category but not the same product
                querysubp =     """SELECT product_id FROM products
                                            WHERE sub_category LIKE '""" + subcategory[0] + """'
                                            AND product_id NOT LIKE CAST(""" + productid + """ AS varchar)
                                            AND gender LIKE '""" + gender[0] + """' OR gender LIKE 'Unisex'
                                            ORDER BY random () LIMIT 1;"""
                c.execute(querysubp)
                productsub = c.fetchone()
                prodids += productsub
                # in this if statement we choose different products from the same category but not the same product
                querysubsubp = """SELECT product_id FROM products
                                            WHERE sub_sub_category LIKE '""" + subsubcategory[0] + """'
                                            AND product_id NOT LIKE CAST(""" + productid + """ AS varchar)
                                            AND gender LIKE '""" + gender[0] + """' OR gender LIKE 'Unisex'
                                            ORDER BY random () LIMIT 1;"""
                c.execute(querysubsubp)
                productsubsub = c.fetchone()
                prodids += productsubsub

            return prodids

        if recom_type == 1:
            # selecting the most sold products
            querypop = """SELECT (products.product_id) FROM products
                                                INNER JOIN pop_products on products.product_id = pop_products.product_id
                                                ORDER BY pop_products.freq DESC
                                                LIMIT 10;"""
            # selecting the best discount on products
            queryprice = """SELECT product_id, discount FROM products
                                                 WHERE discount IS NOT NULL
                                                 ORDER BY discount DESC
                                                 limit 10;"""
            c.execute(querypop)
            # getting a random sample from de best selling products
            products_best_seller_raw = c.fetchall()
            products_best_seller_id = [i[0] for i in products_best_seller_raw]
            products_best_seller_random = random.sample(products_best_seller_id, 5)

            c.execute(queryprice)
            # getting a random sample from the best discount on products
            products_best_price_raw = c.fetchall()
            products_best_price_id = [i[0] for i in products_best_price_raw]
            products_best_price_random = random.sample(products_best_price_id, 5)
            # combining the two togheter and displaying 4 random products
            products_combined = random.sample(products_best_seller_random + products_best_price_random, 4)
            prodids += products_combined
            return prodids

        if recom_type == 2:
            bought_prod = []
            # selecting the buids from the profile ids
            query_prof = ("""SELECT buids FROM profiles
                                    WHERE profile_id = '%s'"""%(profileid))
            c.execute(query_prof)

            buid = c.fetchone()
            for x in buid[0]:
                # selecting the productid from sessions who bought something
                segmentquery= ("""SELECT product_id FROM sessions
                                                WHERE segment = "BUYER"
                                                AND buid IS (%s)""", x)

                c.execute(segmentquery)
                bought= c.fetchone()
                # bought product_ids added to a list
                if bought is not None:
                    for y in bought[0]:
                        splity = y.split("'")[1]
                        bought_prod.append(splity)
                    else:
                        continue
            for z in range(0, int(count)):
                # personal recommandation from the common bought products  in the shoppingcartpage
                # giving them a random set of similar items that they already bought
                prod_pers = bought_prod[random.randint(0, len(bought_prod)-1)]
                query_sub_sub_prod = """SELECT sub_sub_category FROM products
                                                WHERE product_id = '"""+ prod_pers + "'"
                c.execute= query_sub_sub_prod
                sub_sub_cat_prod = c.fetchone()

                query_gender= "SELECT gender FROM products WHERE productid ='"+ prod_pers + "'"
                c.execute(query_gender)
                gender_prod = c.fetchone()

                query_sub_sub_pers = """SELECT product_id FROM products
                                        WHERE sub_sub_category ='""" + sub_sub_cat_prod[0]+"""'
                                        ORDER BY random () LIMIT 1 """
                c.execute(query_sub_sub_pers)
                personal_prod = c.fetchone()
                prodids += personal_prod
        return prodids

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<int:count>/<string:productid>/<int:recom_type>")